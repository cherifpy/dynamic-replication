from configurations.configuration import Configuration
from experiments.params import (
    PATH_TO_CONFIG_FILE, 
    BD_LISTENING_PORT,
)

config = Configuration(
    config_file_path = PATH_TO_CONFIG_FILE,
)

provider = config.setReservation()
netem = config.setNetworkConstraintes()

#destroy
config.provider.destroy()
