import kopf


def say_yes(value, spec, **_) -> bool:
    return value == 'somevalue' and spec.get('field') is not None


def say_no(value, spec, **_) -> bool:
    return value == 'somevalue' and spec.get('field') == 'not-this-value-for-sure'


@kopf.on.create('kopfexamples', labels={'somelabel': 'somevalue'})
def create_with_labels_matching(logger, **kwargs):
    logger.info("Label is matching.")


@kopf.on.create('kopfexamples', labels={'somelabel': kopf.PRESENT})
def create_with_labels_present(logger, **kwargs):
    logger.info("Label is present.")


@kopf.on.create('kopfexamples', labels={'nonexistent': kopf.ABSENT})
def create_with_labels_absent(logger, **kwargs):
    logger.info("Label is absent.")


@kopf.on.create('kopfexamples', labels={'somelabel': say_yes})
def create_with_labels_callback_matching(logger, **kwargs):
    logger.info("Label callback matching.")


@kopf.on.create('kopfexamples', annotations={'someannotation': 'somevalue'})
def create_with_annotations_matching(logger, **kwargs):
    logger.info("Annotation is matching.")


@kopf.on.create('kopfexamples', annotations={'someannotation': kopf.PRESENT})
def create_with_annotations_present(logger, **kwargs):
    logger.info("Annotation is present.")


@kopf.on.create('kopfexamples', annotations={'nonexistent': kopf.ABSENT})
def create_with_annotations_absent(logger, **kwargs):
    logger.info("Annotation is absent.")


@kopf.on.create('kopfexamples', annotations={'someannotation': say_no})
def create_with_annotations_callback_matching(logger, **kwargs):
    logger.info("Annotation callback mismatch.")


@kopf.on.create('kopfexamples', when=lambda body, **_: True)
def create_with_filter_satisfied(logger, **kwargs):
    logger.info("Filter satisfied.")


@kopf.on.create('kopfexamples', when=lambda body, **_: False)
def create_with_filter_not_satisfied(logger, **kwargs):
    logger.info("Filter not satisfied.")


@kopf.on.create('kopfexamples', field='spec.field', value='value')
def create_with_field_value_satisfied(logger, **kwargs):
    logger.info("Field value is satisfied.")


@kopf.on.create('kopfexamples', field='spec.field', value='something-else')
def create_with_field_value_not_satisfied(logger, **kwargs):
    logger.info("Field value is not satisfied.")


@kopf.on.create('kopfexamples', field='spec.field', value=kopf.PRESENT)
def create_with_field_presence_satisfied(logger, **kwargs):
    logger.info("Field presence is satisfied.")


@kopf.on.create('kopfexamples', field='spec.inexistent', value=kopf.PRESENT)
def create_with_field_presence_not_satisfied(logger, **kwargs):
    logger.info("Field presence is not satisfied.")


@kopf.on.update('kopfexamples',
                field='spec.field', old='value', new='changed')
def update_with_field_change_satisfied(logger, **kwargs):
    logger.info("Field change is satisfied.")


@kopf.daemon('kopfexamples', field='spec.field', value='value')
def daemon_with_field(stopped, logger, **kwargs):
    while not stopped:
        logger.info("Field daemon is satisfied.")
        stopped.wait(1)
