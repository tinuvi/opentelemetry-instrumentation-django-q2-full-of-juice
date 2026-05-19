import os.path
import uuid

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEBUG = True

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
}

INSTALLED_APPS = (
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.staticfiles",
    "django_q",
    "tests",
)

Q_CLUSTER = {
    "name": "test-cluster",
    "workers": 1,
    "timeout": 60,
    "retry": 90,
    "orm": "default",
    "sync": True,
}

MIDDLEWARE_CLASSES = (
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
)

STATIC_URL = "/static/"

MEDIA_ROOT = os.path.join(BASE_DIR, "media")

SITE_ID = 1
ROOT_URLCONF = "tests.testapp.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "debug": True,
        },
    },
]

SECRET_KEY = uuid.uuid4().hex

USE_TZ = True

TEST_OUTPUT_DIR = os.path.join(BASE_DIR, "..", "..", "tests-reports")
TEST_OUTPUT_FILE_NAME = "junit.xml"
