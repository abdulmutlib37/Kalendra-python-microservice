"""
Firestore client initialisation.

Cloud Run:  uses Application Default Credentials automatically — no
            service-account key file needed.
Local dev:  set GOOGLE_APPLICATION_CREDENTIALS to the path of a
            service-account key JSON file (same one the Node backend uses).

This mirrors calendai-backend/src/config/firebase.js which calls
admin.initializeApp() and relies on the same credential chain.
"""

import os

import firebase_admin
from firebase_admin import credentials, firestore

from app.logging_config import get_logger

_app: firebase_admin.App | None = None


def init_firestore() -> firestore.firestore.Client:
    """Initialise the Firebase Admin SDK and return a Firestore client."""
    global _app
    logger = get_logger()

    if _app is None:
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if cred_path and os.path.exists(cred_path):
            cred = credentials.Certificate(cred_path)
            _app = firebase_admin.initialize_app(cred)
            logger.info("Firestore initialised with service-account key")
        else:
            # Cloud Run injects default credentials automatically
            _app = firebase_admin.initialize_app()
            logger.info("Firestore initialised with default credentials")

    return firestore.client()


def get_db() -> firestore.firestore.Client:
    """Return the Firestore client, initialising on first call."""
    return init_firestore()
