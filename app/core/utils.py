import logging

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    level=logging.ERROR
)
logger = logging.getLogger(__name__)
