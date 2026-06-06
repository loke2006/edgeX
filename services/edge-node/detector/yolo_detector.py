"""
EdgeCloudX Edge Node — YOLOv8 Vehicle Detector
================================================
Uses frozen YOLOv8 nano model for real-time vehicle detection.
Falls back to simulation mode if YOLO is unavailable.
"""

import logging

import numpy as np

logger = logging.getLogger(__name__)


class YOLODetector:
    """
    YOLOv8-based vehicle detector.
    Loads the ultralytics model for inference on traffic camera frames.
    """

    # Vehicle class IDs in COCO dataset
    VEHICLE_CLASSES = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
    EMERGENCY_CLASSES = {2: "car"}  # Will use color detection for ambulance

    def __init__(self, model_path: str = "yolov8n.pt", confidence: float = 0.4):
        self.model = None
        self.model_path = model_path
        self.confidence = confidence
        self._loaded = False

        try:
            from ultralytics import YOLO
            self.model = YOLO(model_path)
            self._loaded = True
            logger.info(f"YOLOv8 model loaded: {model_path}")
        except Exception as e:
            logger.warning(f"YOLOv8 not available, using simulation mode: {e}")

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    def detect(self, frame: np.ndarray) -> dict:
        """
        Run YOLOv8 inference on a frame.

        Returns:
            Dict with vehicle_count, detections list, and anomaly info.
        """
        if not self._loaded or self.model is None:
            return self._simulate_detection(frame)

        try:
            results = self.model(frame, conf=self.confidence, verbose=False)
            detections = []
            vehicle_count = 0

            for result in results:
                for box in result.boxes:
                    cls_id = int(box.cls[0])
                    conf = float(box.conf[0])

                    if cls_id in self.VEHICLE_CLASSES:
                        vehicle_count += 1
                        x1, y1, x2, y2 = box.xyxy[0].tolist()
                        detections.append({
                            "class": self.VEHICLE_CLASSES[cls_id],
                            "confidence": round(conf, 3),
                            "bbox": [int(x1), int(y1), int(x2), int(y2)],
                        })

            # Simple anomaly detection: too many vehicles = potential congestion event
            anomaly_detected = vehicle_count > 20
            anomaly_type = "congestion_spike" if anomaly_detected else None

            return {
                "vehicle_count": vehicle_count,
                "detections": detections,
                "anomaly_detected": anomaly_detected,
                "anomaly_type": anomaly_type,
            }

        except Exception as e:
            logger.error(f"YOLOv8 inference error: {e}")
            return self._simulate_detection(frame)

    def _simulate_detection(self, frame: np.ndarray) -> dict:
        """Fallback simulation when YOLO is not available."""
        # Estimate vehicles from frame brightness/activity
        mean_brightness = np.mean(frame) if frame.size > 0 else 0
        estimated_count = max(0, int(mean_brightness / 15))

        return {
            "vehicle_count": estimated_count,
            "detections": [],
            "anomaly_detected": False,
            "anomaly_type": None,
            "simulated": True,
        }
