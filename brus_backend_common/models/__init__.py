# from brus_backend_common.models.broker_submissions import *
# from brus_backend_common.models.broker_external import *
from brus_backend_common.models.reference import DEFCDelta

# from brus_backend_common.models.usas import *

DELTA_MODEL_CLASSES = [
    DEFCDelta,
]
DELTA_MODELS = {model.table_name: model for model in DELTA_MODEL_CLASSES}
