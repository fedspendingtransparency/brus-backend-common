# from brus_backend_common.models.broker_submissions import *
# from brus_backend_common.models.broker_external import *
from brus_backend_common.models.reference import DEFCDeltaInt, DEFCDeltaRaw, ExternalDataLoadDateDelta

# from brus_backend_common.models.usas import *

DELTA_MODEL_CLASSES = [
    DEFCDeltaRaw,
    DEFCDeltaInt,
    ExternalDataLoadDateDelta,
]
DELTA_MODELS = {model.table_ref: model for model in DELTA_MODEL_CLASSES}
