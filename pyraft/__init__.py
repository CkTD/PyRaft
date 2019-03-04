import logging

logger = logging.getLogger('raft')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(name)-14s %(levelname)-7s %(message)s')

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter)

logger.addHandler(sh)

# fh = logging.FileHandler('raft.log')
# fh.setLevel(logging.DEBUG)
# fh.setFormatter(formatter)
# logger.addHandler(fh)

logging.getLogger('raft.network').setLevel(logging.DEBUG)
logging.getLogger('raft.eventloop').setLevel(logging.INFO)
logging.getLogger('raft.core').setLevel(logging.INFO)