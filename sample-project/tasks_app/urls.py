from django.urls import path

from tasks_app import views

urlpatterns = [
    path("health/", views.health, name="health"),
    path("api/enqueue/", views.enqueue, name="enqueue"),
    path("api/enqueue-chain/", views.enqueue_chain, name="enqueue_chain"),
    path("api/enqueue-iter/", views.enqueue_iter, name="enqueue_iter"),
    path("api/enqueue-detached/", views.enqueue_detached, name="enqueue_detached"),
    path("api/enqueue-with-baggage/", views.enqueue_with_baggage, name="enqueue_with_baggage"),
]
