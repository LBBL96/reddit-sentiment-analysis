import mlflow
import mlflow.pytorch
from typing import Any
import logging
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MLflowTracker:
    def __init__(
        self,
        tracking_uri: str | None = None,
        experiment_name: str | None = None
    ):
        self.tracking_uri = tracking_uri or settings.mlflow_tracking_uri
        self.experiment_name = experiment_name or settings.mlflow_experiment_name
        
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(self.experiment_name)
        
        logger.info(f"MLflow tracking initialized: {self.tracking_uri}")
    
    def start_run(self, run_name: str | None = None, tags: dict[str, str] | None = None):
        mlflow.start_run(run_name=run_name)
        
        if tags:
            mlflow.set_tags(tags)
        
        logger.info(f"Started MLflow run: {mlflow.active_run().info.run_id}")
        return mlflow.active_run()
    
    def log_params(self, params: dict[str, Any]):
        for key, value in params.items():
            mlflow.log_param(key, value)
    
    def log_metrics(self, metrics: dict[str, float], step: int | None = None):
        for key, value in metrics.items():
            mlflow.log_metric(key, value, step=step)
    
    def log_model(self, model, artifact_path: str = "model", **kwargs):
        mlflow.pytorch.log_model(model, artifact_path, **kwargs)
        logger.info(f"Model logged to MLflow: {artifact_path}")
    
    def log_artifact(self, local_path: str, artifact_path: str | None = None):
        mlflow.log_artifact(local_path, artifact_path)
    
    def log_dict(self, dictionary: dict, artifact_file: str):
        mlflow.log_dict(dictionary, artifact_file)
    
    def end_run(self):
        mlflow.end_run()
        logger.info("MLflow run ended")
    
    def get_run_id(self) -> str | None:
        active_run = mlflow.active_run()
        return active_run.info.run_id if active_run else None


class WandBTracker:
    def __init__(self, project: str | None = None, entity: str | None = None):
        try:
            import wandb
            self.wandb = wandb
            self.project = project or settings.wandb_project
            self.entity = entity
            self.run = None
            logger.info(f"W&B tracker initialized for project: {self.project}")
        except ImportError:
            logger.warning("wandb not installed. Install with: pip install wandb")
            self.wandb = None
    
    def init_run(
        self,
        name: str | None = None,
        config: dict | None = None,
        tags: list | None = None
    ):
        if not self.wandb:
            logger.warning("W&B not available, skipping initialization")
            return
        
        self.run = self.wandb.init(
            project=self.project,
            entity=self.entity,
            name=name,
            config=config,
            tags=tags
        )
        logger.info(f"Started W&B run: {self.run.id if self.run else 'N/A'}")
        return self.run
    
    def log_metrics(self, metrics: dict[str, float], step: int | None = None):
        if not self.wandb or not self.run:
            return
        
        self.wandb.log(metrics, step=step)
    
    def log_model(self, model_path: str, name: str = "model"):
        if not self.wandb or not self.run:
            return
        
        artifact = self.wandb.Artifact(name, type='model')
        artifact.add_dir(model_path)
        self.run.log_artifact(artifact)
        logger.info(f"Model logged to W&B: {name}")
    
    def finish(self):
        if self.wandb and self.run:
            self.wandb.finish()
            logger.info("W&B run finished")
    
    def get_run_id(self) -> str | None:
        return self.run.id if self.run else None


class CombinedTracker:
    def __init__(
        self,
        use_mlflow: bool = True,
        use_wandb: bool = True,
        mlflow_tracking_uri: str | None = None,
        mlflow_experiment_name: str | None = None,
        wandb_project: str | None = None
    ):
        self.mlflow_tracker = MLflowTracker(
            tracking_uri=mlflow_tracking_uri,
            experiment_name=mlflow_experiment_name
        ) if use_mlflow else None
        
        self.wandb_tracker = WandBTracker(
            project=wandb_project
        ) if use_wandb else None
    
    def start_run(
        self,
        run_name: str | None = None,
        config: dict | None = None,
        tags: dict[str, str] | None = None
    ):
        if self.mlflow_tracker:
            self.mlflow_tracker.start_run(run_name=run_name, tags=tags)
        
        if self.wandb_tracker:
            self.wandb_tracker.init_run(name=run_name, config=config, tags=list(tags.values()) if tags else None)
    
    def log_params(self, params: dict[str, Any]):
        if self.mlflow_tracker:
            self.mlflow_tracker.log_params(params)
    
    def log_metrics(self, metrics: dict[str, float], step: int | None = None):
        if self.mlflow_tracker:
            self.mlflow_tracker.log_metrics(metrics, step)
        
        if self.wandb_tracker:
            self.wandb_tracker.log_metrics(metrics, step)
    
    def log_model(self, model_path: str, model_object=None):
        if self.mlflow_tracker and model_object:
            self.mlflow_tracker.log_model(model_object, "model")
        
        if self.wandb_tracker:
            self.wandb_tracker.log_model(model_path, "sentiment_model")
    
    def end_run(self):
        if self.mlflow_tracker:
            self.mlflow_tracker.end_run()
        
        if self.wandb_tracker:
            self.wandb_tracker.finish()
    
    def get_run_ids(self) -> dict[str, str | None]:
        return {
            'mlflow_run_id': self.mlflow_tracker.get_run_id() if self.mlflow_tracker else None,
            'wandb_run_id': self.wandb_tracker.get_run_id() if self.wandb_tracker else None
        }
