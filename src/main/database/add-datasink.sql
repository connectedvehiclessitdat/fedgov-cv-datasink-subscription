-- Run the following statements to make System Builder UI tool aware of the datasink.
INSERT INTO APPLICATION.PROCESS_GROUP_CONFIG VALUES(
	'datasink.cvsubscription', 
	'datasink.default', 
	'private', 
	'cv-subscription?.@build.domain@', 
	'ingest.rtws.saic.com', 
	null, 
	'm1.large', 
	'instance', 
	null, 
	'datasink-cvsubscription.ini', 
	'services.cvsubscription.xml', 
	'{"default-num-volumes" : 0, "default-volume-size" : 0, "config-volume-size" : true, "config-persistent-ip" : false, "config-instance-size" : true, "config-min-max" : false, "config-scaling" : false, "config-jms-persistence" : false }'
);
	
INSERT INTO APPLICATION.DATASINK_CONFIG VALUES(
	'gov.usdot.cv.subscription.datasink.SubscriptionProcessor',
	'N',
	'Y',
	0,
	'',
	'',
	'',
	'datasink.cvsubscription'
);	