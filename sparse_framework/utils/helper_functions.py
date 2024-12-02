"""This module contains general purpose helper functions.
"""
import asyncio

async def retry_connection_until_successful(protocol_factory, host, port, logger = None):
    """Tries to connect to the given address with the specified protocol. If the connection is refused, it will be
    retried after a timeout.
    """
    loop = asyncio.get_running_loop()
    on_con_lost = loop.create_future()

    while True:
        try:
            if logger is not None:
                logger.debug("Connecting to root server on %s:%s.", host, port)
            await loop.create_connection(lambda: protocol_factory(on_con_lost), host, port)
            await on_con_lost
            break
        except ConnectionRefusedError:
            if logger is not None:
                logger.warn("Connection refused. Re-trying in 5 seconds.")
            await asyncio.sleep(5)
