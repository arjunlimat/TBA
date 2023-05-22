# -*- coding: utf-8 -*-
from django.urls import path
from .views import Processing

urlpatterns = [
    path("fileVerification/", Processing.as_view(), name="Processing"),
]
