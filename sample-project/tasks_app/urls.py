from django.urls import path

from tasks_app import views

urlpatterns = [
    path("health/", views.health, name="health"),
    path("api/enqueue/", views.enqueue, name="enqueue"),
]
