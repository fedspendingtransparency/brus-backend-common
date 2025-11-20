# from brus_backend_common.models.broker_submissions import *
# from brus_backend_common.models.broker_external import *
from brus_backend_common.models.reference import DEFCDeltaInt, DEFCDeltaRaw, ExternalDataLoadDateDelta

# from brus_backend_common.models.usas import *

DELTA_MODEL_CLASSES = [
    DEFCDeltaRaw,
    DEFCDeltaInt,
    ExternalDataLoadDateDelta,
]
for model in DELTA_MODEL_CLASSES:
    # force call of the @property getter instead of the simpler list comprehension
    table_ref = model.table_ref
    DELTA_MODELS[table_ref] = model
