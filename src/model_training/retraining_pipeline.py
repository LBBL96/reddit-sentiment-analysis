from datetime import datetime, timedelta
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_
from src.database.database import SessionLocal
from src.database.models import Tweet, TrainingData, ModelMetrics
from src.model_training.trainer import SentimentModelTrainer, sentiment_to_label
from src.model_training.mlflow_tracking import CombinedTracker
from src.preprocessing.text_processor import preprocess_for_model
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetrainingPipeline:
    def __init__(
        self,
        min_samples: int | None = None,
        model_save_dir: str = "./trained_models"
    ):
        self.min_samples = min_samples or settings.min_samples_for_retrain
        self.model_save_dir = model_save_dir
        self.tracker = CombinedTracker()
    
    def check_retraining_needed(self, db: Session) -> bool:
        new_samples_count = db.query(TrainingData).filter(
            TrainingData.used_for_training == False
        ).count()
        
        logger.info(f"New training samples available: {new_samples_count}")
        
        if new_samples_count >= self.min_samples:
            logger.info(f"Retraining threshold reached ({new_samples_count} >= {self.min_samples})")
            return True
        
        last_training = db.query(ModelMetrics).order_by(
            ModelMetrics.created_at.desc()
        ).first()
        
        if last_training:
            hours_since_training = (datetime.utcnow() - last_training.created_at).total_seconds() / 3600
            if hours_since_training >= settings.retrain_interval_hours:
                logger.info(f"Time-based retraining triggered ({hours_since_training:.1f} hours since last training)")
                return True
        
        return False
    
    def prepare_training_data(self, db: Session) -> tuple:
        training_data = db.query(TrainingData).filter(
            TrainingData.label.isnot(None)
        ).all()
        
        if len(training_data) < self.min_samples:
            logger.warning(f"Insufficient training data: {len(training_data)} samples")
            return None, None
        
        texts = [preprocess_for_model(item.text) for item in training_data]
        labels = [sentiment_to_label(item.label) for item in training_data]
        
        logger.info(f"Prepared {len(texts)} samples for training")
        return texts, labels
    
    def train_new_model(
        self,
        texts: list[str],
        labels: list[int],
        version: str | None = None
    ) -> dict:
        version = version or datetime.now().strftime("%Y%m%d_%H%M%S")
        
        self.tracker.start_run(
            run_name=f"sentiment_model_{version}",
            config={
                'model_name': settings.model_name,
                'max_length': settings.max_length,
                'batch_size': settings.batch_size,
                'learning_rate': settings.learning_rate,
                'num_epochs': settings.num_epochs,
                'num_samples': len(texts)
            },
            tags={'version': version, 'type': 'retraining'}
        )
        
        trainer = SentimentModelTrainer()
        
        train_dataset, val_dataset, test_dataset = trainer.prepare_data(texts, labels)
        
        self.tracker.log_params({
            'train_samples': len(train_dataset),
            'val_samples': len(val_dataset),
            'test_samples': len(test_dataset)
        })
        
        train_metrics = trainer.train(train_dataset, val_dataset)
        
        self.tracker.log_metrics({
            'train_loss': train_metrics.get('train_loss', 0),
            'train_runtime': train_metrics.get('train_runtime', 0)
        })
        
        eval_metrics = trainer.evaluate(test_dataset)
        
        self.tracker.log_metrics({
            'test_accuracy': eval_metrics.get('eval_accuracy', 0),
            'test_precision': eval_metrics.get('eval_precision', 0),
            'test_recall': eval_metrics.get('eval_recall', 0),
            'test_f1': eval_metrics.get('eval_f1', 0)
        })
        
        model_path = trainer.save_model(self.model_save_dir, version)
        
        self.tracker.log_model(model_path, trainer.model)
        
        run_ids = self.tracker.get_run_ids()
        
        self.tracker.end_run()
        
        return {
            'version': version,
            'model_path': model_path,
            'metrics': eval_metrics,
            'mlflow_run_id': run_ids.get('mlflow_run_id'),
            'wandb_run_id': run_ids.get('wandb_run_id'),
            'training_samples': len(texts)
        }
    
    def save_model_metrics(self, db: Session, training_result: dict):
        metrics = training_result['metrics']
        
        model_metrics = ModelMetrics(
            model_version=training_result['version'],
            model_path=training_result['model_path'],
            accuracy=metrics.get('eval_accuracy', 0),
            precision=metrics.get('eval_precision', 0),
            recall=metrics.get('eval_recall', 0),
            f1_score=metrics.get('eval_f1', 0),
            training_samples=training_result['training_samples'],
            mlflow_run_id=training_result.get('mlflow_run_id'),
            wandb_run_id=training_result.get('wandb_run_id'),
            is_deployed=False
        )
        
        db.add(model_metrics)
        db.commit()
        
        logger.info(f"Saved model metrics for version {training_result['version']}")
    
    def mark_data_as_used(self, db: Session):
        db.query(TrainingData).filter(
            TrainingData.used_for_training == False
        ).update({'used_for_training': True})
        db.commit()
        logger.info("Marked training data as used")
    
    def run_retraining_cycle(self) -> dict | None:
        db = SessionLocal()
        try:
            if not self.check_retraining_needed(db):
                logger.info("Retraining not needed at this time")
                return None
            
            logger.info("Starting retraining cycle...")
            
            texts, labels = self.prepare_training_data(db)
            if texts is None:
                return None
            
            training_result = self.train_new_model(texts, labels)
            
            self.save_model_metrics(db, training_result)
            
            self.mark_data_as_used(db)
            
            logger.info(f"Retraining cycle completed successfully. Model version: {training_result['version']}")
            return training_result
            
        except Exception as e:
            logger.error(f"Error during retraining cycle: {e}")
            return None
        finally:
            db.close()
    
    def get_best_model(self, db: Session) -> ModelMetrics | None:
        best_model = db.query(ModelMetrics).order_by(
            ModelMetrics.f1_score.desc()
        ).first()
        
        return best_model


class DataQualityMonitor:
    def __init__(self, db: Session):
        self.db = db
    
    def check_data_drift(self, window_hours: int = 24) -> dict:
        cutoff_time = datetime.utcnow() - timedelta(hours=window_hours)
        
        recent_tweets = self.db.query(Tweet).filter(
            and_(
                Tweet.created_at >= cutoff_time,
                Tweet.sentiment_label.isnot(None)
            )
        ).all()
        
        if not recent_tweets:
            return {'has_drift': False, 'message': 'No recent data'}
        
        sentiment_distribution = {}
        for tweet in recent_tweets:
            label = tweet.sentiment_label
            sentiment_distribution[label] = sentiment_distribution.get(label, 0) + 1
        
        total = len(recent_tweets)
        distribution_percentages = {
            label: (count / total) * 100 
            for label, count in sentiment_distribution.items()
        }
        
        has_drift = False
        if max(distribution_percentages.values()) > 70:
            has_drift = True
        
        return {
            'has_drift': has_drift,
            'distribution': distribution_percentages,
            'total_samples': total,
            'window_hours': window_hours
        }
    
    def check_model_performance(self, window_hours: int = 24) -> dict:
        cutoff_time = datetime.utcnow() - timedelta(hours=window_hours)
        
        recent_predictions = self.db.query(Tweet).filter(
            and_(
                Tweet.created_at >= cutoff_time,
                Tweet.sentiment_label.isnot(None),
                Tweet.confidence.isnot(None)
            )
        ).all()
        
        if not recent_predictions:
            return {'status': 'no_data', 'message': 'No recent predictions'}
        
        confidences = [tweet.confidence for tweet in recent_predictions]
        avg_confidence = sum(confidences) / len(confidences)
        
        low_confidence_count = sum(1 for c in confidences if c < 0.6)
        low_confidence_ratio = low_confidence_count / len(confidences)
        
        performance_degraded = avg_confidence < 0.7 or low_confidence_ratio > 0.3
        
        return {
            'status': 'degraded' if performance_degraded else 'healthy',
            'avg_confidence': avg_confidence,
            'low_confidence_ratio': low_confidence_ratio,
            'total_predictions': len(recent_predictions)
        }
