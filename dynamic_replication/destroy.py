from configurations.configuration import Configuration
from exp.params import (
    PATH_TO_CONFIG_FILE, 
    MEMCACHED_LISTENING_PORT,
)

config = Configuration(
    config_file_path = PATH_TO_CONFIG_FILE,
    memcached_listening_port=MEMCACHED_LISTENING_PORT
)

provider = config.setReservation()
netem = config.setNetworkConstraintes()

#destroy
config.provider.destroy()
