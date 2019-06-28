from django.conf.urls import include, url
from django.contrib import admin
from web import views as web_views
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

urlpatterns = [
    # Examples:
    # url(r'^$', 'demo.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    #url(r'^admin/', include(admin.site.urls)),
    url(r'^$',web_views.index),
    url(r'^2',web_views.index2),
    url(r'^3',web_views.index3),
    url(r'^4',web_views.index4),
    url(r'^5',web_views.index5),
    url(r'^6',web_views.index6),
    url(r'^i2o',web_views.i2o),
    url(r'^i3o',web_views.i3o),
    url(r'^i4o',web_views.i4o),
    url(r'^i5o',web_views.i5o),
    url(r'^i6o',web_views.i6o),
    url(r'^7',web_views.index7),
    url(r'^8',web_views.index8),
]

#urlpatterns += staticfiles_urlpatterns()