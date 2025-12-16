from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime
from sqlalchemy.orm import Session
import logging
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

from src.database.database import get_db, init_db
from src.database.models import Tweet, ModelMetrics
from src.model_training.trainer import SentimentModelTrainer, label_to_sentiment
from src.model_training.retraining_pipeline import RetrainingPipeline, DataQualityMonitor
from src.preprocessing.text_processor import preprocess_for_model
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Twitter Sentiment Analysis API",
    description="Real-time sentiment analysis API for Twitter data",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

prediction_counter = Counter('sentiment_predictions_total', 'Total sentiment predictions')
prediction_duration = Histogram('sentiment_prediction_duration_seconds', 'Prediction duration')
model_confidence = Histogram('sentiment_prediction_confidence', 'Prediction confidence scores')

model_trainer: SentimentModelTrainer | None = None


class PredictionRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=500, description="Text to analyze")


class BatchPredictionRequest(BaseModel):
    texts: list[str] = Field(..., max_items=100, description="List of texts to analyze")


class PredictionResponse(BaseModel):
    text: str
    sentiment: str
    label: int
    confidence: float
    probabilities: dict[str, float]
    model_version: str | None = None
    timestamp: datetime


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    model_version: str | None = None
    timestamp: datetime


class RetrainingRequest(BaseModel):
    force: bool = Field(default=False, description="Force retraining even if not needed")


@app.on_event("startup")
async def startup_event():
    global model_trainer
    logger.info("Initializing database...")
    init_db()
    
    logger.info("Loading sentiment model...")
    try:
        model_trainer = SentimentModelTrainer()
        
        db = next(get_db())
        latest_model = db.query(ModelMetrics).filter(
            ModelMetrics.is_deployed == True
        ).order_by(ModelMetrics.created_at.desc()).first()
        
        if latest_model:
            model_trainer.load_model(latest_model.model_path)
            logger.info(f"Loaded model version: {latest_model.model_version}")
        else:
            logger.warning("No deployed model found. Using base model.")
        
        db.close()
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        model_trainer = SentimentModelTrainer()


@app.get("/", response_model=dict)
async def root():
    return {
        "message": "Twitter Sentiment Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "predict": "/predict",
            "batch_predict": "/predict/batch",
            "retrain": "/retrain",
            "metrics": "/metrics",
            "model_info": "/model/info"
        }
    }


@app.get("/health", response_model=HealthResponse)
async def health_check(db: Session = Depends(get_db)):
    model_version = None
    if model_trainer and model_trainer.model:
        latest_model = db.query(ModelMetrics).filter(
            ModelMetrics.is_deployed == True
        ).order_by(ModelMetrics.created_at.desc()).first()
        model_version = latest_model.model_version if latest_model else "base"
    
    return HealthResponse(
        status="healthy" if model_trainer else "degraded",
        model_loaded=model_trainer is not None and model_trainer.model is not None,
        model_version=model_version,
        timestamp=datetime.utcnow()
    )


@app.post("/predict", response_model=PredictionResponse)
async def predict_sentiment(request: PredictionRequest, db: Session = Depends(get_db)):
    if not model_trainer or not model_trainer.model:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        with prediction_duration.time():
            preprocessed = preprocess_for_model(request.text)
            predictions = model_trainer.predict([preprocessed])
            result = predictions[0]
        
        sentiment = label_to_sentiment(result['label'])
        
        prediction_counter.inc()
        model_confidence.observe(result['confidence'])
        
        latest_model = db.query(ModelMetrics).filter(
            ModelMetrics.is_deployed == True
        ).order_by(ModelMetrics.created_at.desc()).first()
        model_version = latest_model.model_version if latest_model else "base"
        
        tweet = Tweet(
            text=request.text,
            preprocessed_text=preprocessed,
            sentiment_label=sentiment,
            sentiment_score=result['confidence'],
            confidence=result['confidence'],
            model_version=model_version,
            processed=True
        )
        db.add(tweet)
        db.commit()
        
        return PredictionResponse(
            text=request.text,
            sentiment=sentiment,
            label=result['label'],
            confidence=result['confidence'],
            probabilities={
                'negative': result['probabilities'][0],
                'neutral': result['probabilities'][1],
                'positive': result['probabilities'][2]
            },
            model_version=model_version,
            timestamp=datetime.utcnow()
        )
    
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@app.post("/predict/batch", response_model=list[PredictionResponse])
async def batch_predict_sentiment(
    request: BatchPredictionRequest,
    db: Session = Depends(get_db)
):
    if not model_trainer or not model_trainer.model:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        preprocessed_texts = [preprocess_for_model(text) for text in request.texts]
        predictions = model_trainer.predict(preprocessed_texts)
        
        latest_model = db.query(ModelMetrics).filter(
            ModelMetrics.is_deployed == True
        ).order_by(ModelMetrics.created_at.desc()).first()
        model_version = latest_model.model_version if latest_model else "base"
        
        responses = []
        for text, preprocessed, result in zip(request.texts, preprocessed_texts, predictions):
            sentiment = label_to_sentiment(result['label'])
            
            prediction_counter.inc()
            model_confidence.observe(result['confidence'])
            
            tweet = Tweet(
                text=text,
                preprocessed_text=preprocessed,
                sentiment_label=sentiment,
                sentiment_score=result['confidence'],
                confidence=result['confidence'],
                model_version=model_version,
                processed=True
            )
            db.add(tweet)
            
            responses.append(PredictionResponse(
                text=text,
                sentiment=sentiment,
                label=result['label'],
                confidence=result['confidence'],
                probabilities={
                    'negative': result['probabilities'][0],
                    'neutral': result['probabilities'][1],
                    'positive': result['probabilities'][2]
                },
                model_version=model_version,
                timestamp=datetime.utcnow()
            ))
        
        db.commit()
        return responses
    
    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {str(e)}")


@app.post("/retrain")
async def trigger_retraining(
    request: RetrainingRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    pipeline = RetrainingPipeline()
    
    if not request.force and not pipeline.check_retraining_needed(db):
        return {
            "status": "skipped",
            "message": "Retraining not needed at this time"
        }
    
    background_tasks.add_task(pipeline.run_retraining_cycle)
    
    return {
        "status": "initiated",
        "message": "Retraining process started in background"
    }


@app.get("/model/info")
async def get_model_info(db: Session = Depends(get_db)):
    deployed_model = db.query(ModelMetrics).filter(
        ModelMetrics.is_deployed == True
    ).order_by(ModelMetrics.created_at.desc()).first()
    
    all_models = db.query(ModelMetrics).order_by(
        ModelMetrics.created_at.desc()
    ).limit(5).all()
    
    return {
        "deployed_model": {
            "version": deployed_model.model_version if deployed_model else None,
            "accuracy": deployed_model.accuracy if deployed_model else None,
            "f1_score": deployed_model.f1_score if deployed_model else None,
            "created_at": deployed_model.created_at if deployed_model else None
        } if deployed_model else None,
        "recent_models": [
            {
                "version": model.model_version,
                "accuracy": model.accuracy,
                "f1_score": model.f1_score,
                "is_deployed": model.is_deployed,
                "created_at": model.created_at
            }
            for model in all_models
        ]
    }


@app.get("/data/quality")
async def check_data_quality(db: Session = Depends(get_db)):
    monitor = DataQualityMonitor(db)
    
    drift_info = monitor.check_data_drift(window_hours=24)
    performance_info = monitor.check_model_performance(window_hours=24)
    
    return {
        "data_drift": drift_info,
        "model_performance": performance_info,
        "timestamp": datetime.utcnow()
    }


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True
    )
