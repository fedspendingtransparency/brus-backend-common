from brus_backend_common.models.lakehouse_model import LakeHouseCurrentMigration, ExternalDataLoadDate

from brus_backend_common.models.reference import (
    AgencyBronze,
    CGACGold,
    DEFCBronze,
    DEFCGold,
    DEFCGroup,
    FONBronze,
    FONGold,
    FRECGold,
    ProgramActivityParkBronze,
    ProgramActivityParkGold,
    SubTierAgencyGold,
)

LAKEHOUSE_MODEL_CLASSES = [
    AgencyBronze,
    CGACGold,
    DEFCBronze,
    DEFCGroup,
    DEFCGold,
    ExternalDataLoadDate,
    FONBronze,
    FONGold,
    FRECGold,
    LakeHouseCurrentMigration,
    SubTierAgencyGold,
]
LAKEHOUSE_MODELS = {}
for model in LAKEHOUSE_MODEL_CLASSES:
    # Due to TABLE_REF being only accessible after its initialized (not a class attribute),
    # we're just instantiating a skeleton of it, pulling the attribute, setting it, and then passing the class
    # to let the caller instantiate however they wish
    inst_model = model()
    table_ref = inst_model.TABLE_REF
    LAKEHOUSE_MODELS[table_ref] = model
