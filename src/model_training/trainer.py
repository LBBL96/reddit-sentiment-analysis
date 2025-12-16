import os
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
    EarlyStoppingCallback
)
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from datetime import datetime
import logging
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SentimentDataset(Dataset):
    def __init__(self, texts: list[str], labels: list[int], tokenizer, max_length: int = 128):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_length = max_length
    
    def __len__(self):
        return len(self.texts)
    
    def __getitem__(self, idx):
        text = str(self.texts[idx])
        label = self.labels[idx]
        
        encoding = self.tokenizer(
            text,
            add_special_tokens=True,
            max_length=self.max_length,
            padding='max_length',
            truncation=True,
            return_attention_mask=True,
            return_tensors='pt'
        )
        
        return {
            'input_ids': encoding['input_ids'].flatten(),
            'attention_mask': encoding['attention_mask'].flatten(),
            'labels': torch.tensor(label, dtype=torch.long)
        }


class SentimentModelTrainer:
    def __init__(
        self,
        model_name: str | None = None,
        num_labels: int = 3,
        max_length: int = 128,
        output_dir: str = "./model_checkpoints"
    ):
        self.model_name = model_name or settings.model_name
        self.num_labels = num_labels
        self.max_length = max_length
        self.output_dir = output_dir
        
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = None
        self.trainer = None
        
        os.makedirs(output_dir, exist_ok=True)
    
    def prepare_data(
        self,
        texts: list[str],
        labels: list[int],
        test_size: float = 0.2,
        val_size: float = 0.1
    ) -> tuple[SentimentDataset, SentimentDataset, SentimentDataset]:
        X_train, X_test, y_train, y_test = train_test_split(
            texts, labels, test_size=test_size, random_state=42, stratify=labels
        )
        
        X_train, X_val, y_train, y_val = train_test_split(
            X_train, y_train, test_size=val_size, random_state=42, stratify=y_train
        )
        
        train_dataset = SentimentDataset(X_train, y_train, self.tokenizer, self.max_length)
        val_dataset = SentimentDataset(X_val, y_val, self.tokenizer, self.max_length)
        test_dataset = SentimentDataset(X_test, y_test, self.tokenizer, self.max_length)
        
        logger.info(f"Train samples: {len(train_dataset)}, Val samples: {len(val_dataset)}, Test samples: {len(test_dataset)}")
        
        return train_dataset, val_dataset, test_dataset
    
    def compute_metrics(self, pred):
        labels = pred.label_ids
        preds = pred.predictions.argmax(-1)
        
        precision, recall, f1, _ = precision_recall_fscore_support(
            labels, preds, average='weighted', zero_division=0
        )
        acc = accuracy_score(labels, preds)
        
        return {
            'accuracy': acc,
            'precision': precision,
            'recall': recall,
            'f1': f1
        }
    
    def train(
        self,
        train_dataset: SentimentDataset,
        val_dataset: SentimentDataset,
        learning_rate: float | None = None,
        num_epochs: int | None = None,
        batch_size: int | None = None,
        warmup_steps: int = 500,
        weight_decay: float = 0.01,
        early_stopping_patience: int = 3
    ) -> dict:
        learning_rate = learning_rate or settings.learning_rate
        num_epochs = num_epochs or settings.num_epochs
        batch_size = batch_size or settings.batch_size
        
        self.model = AutoModelForSequenceClassification.from_pretrained(
            self.model_name,
            num_labels=self.num_labels
        )
        
        training_args = TrainingArguments(
            output_dir=self.output_dir,
            num_train_epochs=num_epochs,
            per_device_train_batch_size=batch_size,
            per_device_eval_batch_size=batch_size,
            warmup_steps=warmup_steps,
            weight_decay=weight_decay,
            learning_rate=learning_rate,
            logging_dir=f'{self.output_dir}/logs',
            logging_steps=100,
            eval_strategy='epoch',
            save_strategy='epoch',
            load_best_model_at_end=True,
            metric_for_best_model='f1',
            greater_is_better=True,
            save_total_limit=3,
            report_to=['none']
        )
        
        self.trainer = Trainer(
            model=self.model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=val_dataset,
            compute_metrics=self.compute_metrics,
            callbacks=[EarlyStoppingCallback(early_stopping_patience=early_stopping_patience)]
        )
        
        logger.info("Starting training...")
        train_result = self.trainer.train()
        
        logger.info("Training completed!")
        return train_result.metrics
    
    def evaluate(self, test_dataset: SentimentDataset) -> dict:
        if self.trainer is None:
            raise ValueError("Model must be trained before evaluation")
        
        logger.info("Evaluating model...")
        eval_result = self.trainer.evaluate(test_dataset)
        
        return eval_result
    
    def save_model(self, save_path: str, version: str | None = None):
        if self.model is None:
            raise ValueError("No model to save")
        
        version = version or datetime.now().strftime("%Y%m%d_%H%M%S")
        full_path = os.path.join(save_path, f"sentiment_model_{version}")
        
        os.makedirs(full_path, exist_ok=True)
        
        self.model.save_pretrained(full_path)
        self.tokenizer.save_pretrained(full_path)
        
        logger.info(f"Model saved to {full_path}")
        return full_path
    
    def load_model(self, model_path: str):
        logger.info(f"Loading model from {model_path}")
        self.model = AutoModelForSequenceClassification.from_pretrained(model_path)
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        
    def predict(self, texts: list[str]) -> list[dict]:
        if self.model is None:
            raise ValueError("Model must be loaded or trained before prediction")
        
        self.model.eval()
        dataset = SentimentDataset(
            texts,
            [0] * len(texts),
            self.tokenizer,
            self.max_length
        )
        
        dataloader = DataLoader(dataset, batch_size=settings.batch_size)
        predictions = []
        
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model.to(device)
        
        with torch.no_grad():
            for batch in dataloader:
                input_ids = batch['input_ids'].to(device)
                attention_mask = batch['attention_mask'].to(device)
                
                outputs = self.model(input_ids=input_ids, attention_mask=attention_mask)
                logits = outputs.logits
                probs = torch.nn.functional.softmax(logits, dim=-1)
                preds = torch.argmax(probs, dim=-1)
                
                for pred, prob in zip(preds, probs):
                    predictions.append({
                        'label': pred.item(),
                        'confidence': prob.max().item(),
                        'probabilities': prob.cpu().numpy().tolist()
                    })
        
        return predictions


def label_to_sentiment(label: int) -> str:
    sentiment_map = {0: 'negative', 1: 'neutral', 2: 'positive'}
    return sentiment_map.get(label, 'unknown')


def sentiment_to_label(sentiment: str) -> int:
    label_map = {'negative': 0, 'neutral': 1, 'positive': 2}
    return label_map.get(sentiment.lower(), 1)
