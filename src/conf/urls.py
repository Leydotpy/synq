from django.contrib import admin
from django.urls import include, path

from conf.health import liveness, readiness

urlpatterns = [
    path("health/live/", liveness, name="health-live"),
    path("health/ready/", readiness, name="health-ready"),
    path("admin/", admin.site.urls),
    path("api/v1/meetings/", include("apps.meetings.api.urls")),
]
