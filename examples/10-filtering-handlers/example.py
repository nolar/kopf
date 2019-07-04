import kopf


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': 'somevalue'})
def create_with_labels_satisfied(logger, **kwargs):
    logger.info("Label satisfied.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': None})
def create_with_labels_exist(logger, **kwargs):
    logger.info("Label exists.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': 'othervalue'})
def create_with_labels_not_satisfied(logger, **kwargs):
    logger.info("Label not satisfied.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': 'somevalue'})
def create_with_annotations_satisfied(logger, **kwargs):
    logger.info("Annotation satisfied.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': None})
def create_with_annotations_exist(logger, **kwargs):
    logger.info("Annotation exists.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': 'othervalue'})
def create_with_annotations_not_satisfied(logger, **kwargs):
    logger.info("Annotation not satisfied.")
