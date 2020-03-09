import kopf


def say_yes(value, spec, **_):
    return value == 'somevalue' and spec.get('field') is not None


def say_no(value, spec, **_):
    return value == 'somevalue' and spec.get('field') == 'not-this-value-for-sure'


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': 'somevalue'})
def create_with_labels_matching(logger, **kwargs):
    logger.info("Label is matching.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': kopf.PRESENT})
def create_with_labels_present(logger, **kwargs):
    logger.info("Label is present.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'nonexistent': kopf.ABSENT})
def create_with_labels_absent(logger, **kwargs):
    logger.info("Label is absent.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', labels={'somelabel': say_yes})
def create_with_labels_callback_matching(logger, **kwargs):
    logger.info("Label callback matching.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': 'somevalue'})
def create_with_annotations_matching(logger, **kwargs):
    logger.info("Annotation is matching.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': kopf.PRESENT})
def create_with_annotations_present(logger, **kwargs):
    logger.info("Annotation is present.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'nonexistent': kopf.ABSENT})
def create_with_annotations_absent(logger, **kwargs):
    logger.info("Annotation is absent.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', annotations={'someannotation': say_no})
def create_with_annotations_callback_matching(logger, **kwargs):
    logger.info("Annotation callback mismatch.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', when=lambda body, **_: True)
def create_with_filter_satisfied(logger, **kwargs):
    logger.info("Filter satisfied.")


@kopf.on.create('zalando.org', 'v1', 'kopfexamples', when=lambda body, **_: False)
def create_with_filter_not_satisfied(logger, **kwargs):
    logger.info("Filter not satisfied.")
