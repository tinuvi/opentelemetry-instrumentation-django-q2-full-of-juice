from django.urls import include, path

urlpatterns = [
    path("", include("tasks_app.urls")),
]
