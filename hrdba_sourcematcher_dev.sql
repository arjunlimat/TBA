insert into configmanager.properties (application, profile, label, key, value) values ('TBASOURCEMATCHER-SERVICE', 'dev', 'latest', 'EUREKA_URL', 'http://10.64.213.15:9001/eureka');


insert into configmanager.properties (application, profile, label, key, value) values ('TBASOURCEMATCHER-SERVICE', 'dev', 'latest', 'REDIS_URL', 'http://10.64.213.15:7777/redis/file_validation/get/');

insert into configmanager.properties (application, profile, label, key, value) values ('TBASOURCEMATCHER-SERVICE', 'dev', 'latest', 'TBA_INQUIRY_URL', 'http://10.64.213.13:8084/inquiry/generalInquiry/');

insert into configmanager.properties (application, profile, label, key, value) values ('TBASOURCEMATCHER-SERVICE', 'dev', 'latest', 'TBA_API_URL', 'https://10.88.132.42/api/TBAInquiry');

insert into configmanager.properties (application, profile, label, key, value) values ('TBASOURCEMATCHER-SERVICE', 'dev', 'latest', 'DB_SERVICE', 'http://10.64.213.15:7777/dbservice/api/v1/maestroticket/maestro/TBA_data_mismatch/dell/Common');

-- FIXME this is direct url to server give zule url
insert into configmanager.properties (application, profile, label, key, value) values ('TBASOURCEMATCHER-SERVICE', 'dev', 'latest', 'RULE_ENGINE_URL', 'http://10.64.213.14:3000/fileinterfaces');
