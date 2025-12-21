'''
Here's plan for the server - python server, you checkin with it,
it returns a key you use to make a websocket connection with the pubsub server.

# TODO:
- [ ] implement DTOs for all the server calls
- [ ] implement Swagger on the server / python packages...
{
    "DTO": "Proposal",
    "error": null,
    "data": {
        "id": 1,
        "author": "22a85fb71485c6d7c62a3784c5549bd3849d0afa3ee44ce3f9ea5541e4c56402d8",
        "title": "Proposal Title",
        "description": "Proposal Description",
        ...
    }
}
JSON -> EXTRACT DATA -> Python Object -> DTO -> JSON
{{ proposal.author }}
'''
from typing import Union
from functools import partial
import base64
import time
import json
import requests
from satorilib import logging
from satorilib.utils.time import timeToTimestamp
from satorilib.wallet import Wallet
from satorilib.concepts.structs import Stream, StreamId
from satorilib.server.api import ProposalSchema, VoteSchema
from satorilib.utils.json import sanitizeJson
from requests.exceptions import RequestException
import json
import traceback
import datetime as dt


def _get_networking_mode() -> str:
    """Get the current networking mode from environment or config."""
    import os
    mode = os.environ.get('SATORI_NETWORKING_MODE')
    if mode is None:
        try:
            from satorilib import config
            mode = config.get().get('networking mode', 'central')
        except Exception:
            mode = 'central'
    return mode.lower().strip()


class SatoriServerClient(object):
    def __init__(
        self,
        wallet: Wallet,
        url: str = None,
        sendingUrl: str = None,
        networking_mode: str = None,
        *args, **kwargs
    ):
        self.wallet = wallet
        self.url = url or 'https://central.satorinet.io'
        self.sendingUrl = sendingUrl or 'https://mundo.satorinet.io'
        self.topicTime: dict[str, float] = {}
        self.lastCheckin: int = 0
        # P2P Integration
        self.networking_mode = networking_mode or _get_networking_mode()
        self._p2p_client = None  # Lazy initialized

    def _get_p2p_client(self):
        """Lazy initialize P2P client for hybrid/p2p modes."""
        if self._p2p_client is None and self.networking_mode in ('hybrid', 'p2p'):
            try:
                from satorip2p.peers import Peers
                from satorip2p.protocol.peer_registry import PeerRegistry
                from satorip2p.protocol.stream_registry import StreamRegistry
                from satorip2p.protocol.oracle_network import OracleNetwork
                from satorip2p.protocol.prediction_protocol import PredictionProtocol
                # New protocol features
                from satorip2p.protocol.versioning import (
                    ProtocolVersion, VersionNegotiator, PeerVersionTracker,
                    PROTOCOL_VERSION, get_current_version
                )
                from satorip2p.protocol.storage import (
                    StorageManager, DeferredRewardsStorage, AlertStorage
                )
                from satorip2p.protocol.bandwidth import (
                    BandwidthTracker, QoSManager, create_qos_manager
                )

                self._p2p_client = {
                    'peers': None,  # Will be initialized by Neuron
                    'peer_registry': None,
                    'stream_registry': None,
                    'oracle_network': None,
                    'prediction_protocol': None,
                    # Existing managers
                    'alert_manager': None,
                    'deferred_rewards_manager': None,
                    # New protocol feature managers
                    'version_tracker': None,
                    'version_negotiator': None,
                    'storage_manager': None,
                    'bandwidth_tracker': None,
                    'qos_manager': None,
                    'protocol_version': PROTOCOL_VERSION,
                }
                logging.info("P2P client structures initialized", color="green")
            except ImportError as e:
                logging.warning(f"satorip2p not available: {e}")
        return self._p2p_client

    def set_p2p_peers(self, peers):
        """Set the P2P peers instance from Neuron."""
        client = self._get_p2p_client()
        if client:
            client['peers'] = peers

    def is_p2p_available(self) -> bool:
        """Check if P2P is available and initialized."""
        client = self._get_p2p_client()
        return client is not None and client.get('peers') is not None

    def setTopicTime(self, topic: str):
        self.topicTime[topic] = time.time()

    def _getChallenge(self):
        # return requests.get(self.url + '/time').text
        return str(time.time())

    def _makeAuthenticatedCall(
        self,
        function: callable,
        endpoint: str,
        url: str = None,
        payload: Union[str, dict, None] = None,
        challenge: str = None,
        useWallet: Wallet = None,
        extraHeaders: Union[dict, None] = None,
        raiseForStatus: bool = True,
    ) -> requests.Response:
        if isinstance(payload, dict):
            payload = json.dumps(payload)

        if payload is not None:
            logging.info(
                f'outgoing: {endpoint}',
                payload[0:40], f'{"..." if len(payload) > 40 else ""}',
                print=True)
        r = function(
            (url or self.url) + endpoint,
            headers={
                **(useWallet or self.wallet).authPayload(
                    asDict=True,
                    challenge=challenge or self._getChallenge()),
                **(extraHeaders or {}),
            },
            json=payload)
        if raiseForStatus:
            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logging.error('authenticated server err:',
                              r.text, e, color='red')
                r.raise_for_status()
        logging.info(
            f'incoming: {endpoint}',
            r.text[0:40], f'{"..." if len(r.text) > 40 else ""}',
            print=True)
        return r

    def _makeUnauthenticatedCall(
        self,
        function: callable,
        endpoint: str,
        url: str = None,
        headers: Union[dict, None] = None,
        payload: Union[str, bytes, None] = None,
    ):
        logging.info(
            'outgoing Satori server message to ',
            endpoint,
            print=True)
        data = None
        json = None
        if isinstance(payload, bytes):
            headers = headers or {'Content-Type': 'application/octet-stream'}
            data = payload
        elif isinstance(payload, str):
            headers = headers or {'Content-Type': 'application/json'}
            json = payload
        else:
            headers = headers or {}
        r = function(
            (url or self.url) + endpoint,
            headers=headers,
            json=json,
            data=data)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error("unauth'ed server err:", r.text, e, color='red')
            r.raise_for_status()
        logging.info(
            'incoming Satori server message:',
            r.text[0:40], f'{"..." if len(r.text) > 40 else ""}',
            print=True)
        return r

    def registerWallet(self):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/wallet',
            payload=self.wallet.registerPayload())

    def registerStream(self, stream: dict, payload: str = None):
        ''' publish stream {'source': 'test', 'name': 'stream1', 'target': 'target'}'''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/stream',
            payload=payload or json.dumps(stream))

    def registerSubscription(self, subscription: dict, payload: str = None):
        ''' subscribe to stream '''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/subscription',
            payload=payload or json.dumps(subscription))

    def registerPin(self, pin: dict, payload: str = None):
        '''
        report a pin to the server.
        example: {
            'author': {'pubkey': '22a85fb71485c6d7c62a3784c5549bd3849d0afa3ee44ce3f9ea5541e4c56402d8'},
            'stream': {'source': 'satori', 'pubkey': '22a85fb71485c6d7c62a3784c5549bd3849d0afa3ee44ce3f9ea5541e4c56402d8', 'stream': 'stream1', 'target': 'target', 'cadence': None, 'offset': None, 'datatype': None, 'url': None, 'description': 'raw data'},,
            'ipns': 'ipns',
            'ipfs': 'ipfs',
            'disk': 1,
            'count': 27},
        '''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/pin',
            payload=payload or json.dumps(pin))

    def requestPrimary(self):
        ''' subscribe to primary data stream and and publish prediction '''
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/request/primary')

    def getStreams(self, stream: dict, payload: str = None):
        ''' subscribe to primary data stream and and publish prediction '''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/get/streams',
            payload=payload or json.dumps(stream))

    def myStreams(self):
        ''' subscribe to primary data stream and and publish prediction '''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/my/streams',
            payload='{}')

    def removeStream(self, stream: dict = None, payload: str = None):
        ''' removes a stream from the server '''
        if payload is None and stream is None:
            raise ValueError('stream or payload must be provided')
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/remove/stream',
            payload=payload or json.dumps(stream or {}))
    
    def restoreStream(self, stream: dict = None, payload: str = None):
        ''' removes a stream from the server '''
        if payload is None and stream is None:
            raise ValueError('stream or payload must be provided')
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/restore/stream',
            payload=payload or json.dumps(stream or {}))

    def checkin(self, referrer: str = None, ip: str = None, vaultInfo: dict = None) -> dict:
        """
        Check in with the network.

        In hybrid mode: tries P2P first, falls back to central.
        In p2p mode: only uses P2P announcement.
        In central mode: only uses central server.
        """
        result = {}

        # P2P announcement for hybrid/p2p modes
        if self.networking_mode in ('hybrid', 'p2p') and self.is_p2p_available():
            try:
                import asyncio
                from satorip2p.protocol.peer_registry import PeerRegistry

                client = self._get_p2p_client()
                if client and client.get('peers'):
                    if client.get('peer_registry') is None:
                        client['peer_registry'] = PeerRegistry(client['peers'])

                    async def _announce():
                        await client['peer_registry'].start()
                        return await client['peer_registry'].announce(capabilities=["predictor"])

                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.create_task(_announce())
                        else:
                            loop.run_until_complete(_announce())
                    except RuntimeError:
                        asyncio.run(_announce())

                    logging.info("P2P announcement completed", color="green")
                    result['p2p_announced'] = True

            except Exception as e:
                logging.warning(f"P2P announcement failed: {e}")
                result['p2p_error'] = str(e)

        # Central server checkin for central/hybrid modes
        if self.networking_mode in ('central', 'hybrid'):
            try:
                challenge = self._getChallenge()
                response = self._makeAuthenticatedCall(
                    function=requests.post,
                    endpoint='/checkin',
                    payload=self.wallet.registerPayload(challenge=challenge, vaultInfo=vaultInfo),
                    challenge=challenge,
                    extraHeaders={
                        **({'referrer': referrer} if referrer else {}),
                        **({'ip': ip} if ip else {})},
                    raiseForStatus=False)
                try:
                    response.raise_for_status()
                    self.lastCheckin = time.time()
                    result.update(response.json())
                except requests.exceptions.HTTPError as e:
                    logging.error('unable to checkin:', response.text, e, color='red')
                    if self.networking_mode == 'central':
                        return {'ERROR': response.text}
                    result['central_error'] = response.text
            except Exception as e:
                logging.warning(f"Central server checkin failed: {e}")
                if self.networking_mode == 'central':
                    return {'ERROR': str(e)}
                result['central_error'] = str(e)

        return result

    def checkinCheck(self) -> bool:
        challenge = self._getChallenge()
        response = self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/checkin/check',
            payload=self.wallet.registerPayload(challenge=challenge),
            challenge=challenge,
            extraHeaders={'changesSince': timeToTimestamp(self.lastCheckin)},
            raiseForStatus=False)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error('unable to checkin:', response.text, e, color='red')
            return False
        return response.text.lower() == 'true'

    def requestSimplePartial(self, network: str):
        ''' sends a satori partial transaction to the server '''
        return self._makeUnauthenticatedCall(
            function=requests.get,
            url=self.sendingUrl,
            endpoint=f'/simple_partial/request/{network}').json()

    def broadcastSimplePartial(
        self,
        tx: bytes,
        feeSatsReserved: float,
        reportedFeeSats: float,
        walletId: float,
        network: str
    ):
        ''' sends a satori partial transaction to the server '''
        return self._makeUnauthenticatedCall(
            function=requests.post,
            url=self.sendingUrl,
            endpoint=f'/simple_partial/broadcast/{network}/{feeSatsReserved}/{reportedFeeSats}/{walletId}',
            payload=tx)

    def broadcastBridgeSimplePartial(
        self,
        tx: bytes,
        feeSatsReserved: float,
        reportedFeeSats: float,
        walletId: float,
        network: str
    ):
        ''' sends a satori partial transaction to the server '''
        return self._makeUnauthenticatedCall(
            function=requests.post,
            url=self.sendingUrl,
            endpoint=f'/simple/bridge/partial/broadcast/{network}/{feeSatsReserved}/{reportedFeeSats}/{walletId}',
            payload=tx)

    def removeWalletAlias(self):
        ''' removes the wallet alias from the server '''
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/remove_wallet_alias')

    def updateWalletAlias(self, alias: str):
        ''' removes the wallet alias from the server '''
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/update_wallet_alias/' + alias)

    def getWalletAlias(self):
        ''' removes the wallet alias from the server '''
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/get_wallet_alias').text

    def getManifestVote(self, wallet: Wallet = None):
        return self._makeUnauthenticatedCall(
            function=requests.get,
            endpoint=(
                f'/votes_for/manifest/{wallet.publicKey}'
                if isinstance(wallet, Wallet) else '/votes_for/manifest')).json()

    def getSanctionVote(self, wallet: Wallet = None, vault: Wallet = None):
        # logging.debug('vault', vault, color='yellow')
        walletPubkey = wallet.publicKey if isinstance(
            wallet, Wallet) else 'None'
        vaultPubkey = vault.publicKey if isinstance(vault, Wallet) else 'None'
        # logging.debug(
        #    f'/votes_for/sanction/{walletPubkey}/{vaultPubkey}', color='yellow')
        return self._makeUnauthenticatedCall(
            function=requests.get,
            endpoint=f'/votes_for/sanction/{walletPubkey}/{vaultPubkey}').json()

    def getSearchStreams(self, searchText: str = None):
        '''
        returns [{
            'author': 27790,
            'cadence': 600.0,
            'datatype': 'float',
            'description': 'Price AED 10min interval coinbase',
            'oracle_address': 'EHJKq4EW2GfGBvhweasMXCZBVbAaTuDERS',
            'oracle_alias': 'WilQSL_x10',
            'oracle_pubkey': '03e3f3a15c2e174cac7ef8d1d9ff81e9d4ef7e33a59c20cc5cc142f9c69493f306',
            'predicting_id': 0,
            'sanctioned': 0,
            'source': 'satori',
            'stream': 'Coinbase.AED.USDT',
            'stream_created_ts': 'Tue, 09 Jul 2024 10:20:11 GMT',
            'stream_id': 326076,
            'tags': 'AED, coinbase',
            'target': 'data.rates.AED',
            'total_vote': 6537.669052915435,
            'url': 'https://api.coinbase.com/v2/exchange-rates',
            'utc_offset': 227.0,
            'vote': 33.333333333333336},...]
        '''

        def cleanAndSort(streams: str, searchText: str = None):
            # Commenting down as of now, will be used in future if we need to make the call to server for search streams
            # as of now we have limited streams so we can search in client side
            # if searchText:
            #     searchedStreams = [s for s in streams if searchText.lower() in s['stream'].lower()]
            #     return sanitizeJson(searchedStreams)
            sanitizedStreams = sanitizeJson(streams)
            # sorting streams based on vote and total_vote
            sortedStreams = sorted(
                sanitizedStreams,
                key=lambda x: (x.get('vote', 0) == 0, -
                               x.get('vote', 0), -x.get('total_vote', 0))
            )
            return sortedStreams

        return cleanAndSort(
            streams=self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/streams/search',
                payload=json.dumps({'address': self.wallet.address})).json(),
            searchText=searchText)
    
    def getSearchStreamsPaginated(self, searchText: str = None, page: int = 1, per_page: int = 100, 
                            sort_by: str = 'popularity', order: str = 'desc') -> tuple[list, int]:
        """ Get streams with full pagination information """
        def cleanAndSort(streams: list, searchText: str = None):
            """Clean and sanitize stream data"""
            return sanitizeJson(streams)

        print("getSearchStreamsPaginated")
        try:
            page = max(1, page)
            per_page = min(max(1, per_page), 200)
            payload = {
                'page': page,
                'per_page': per_page,
                'sort': sort_by,
                'order': order
            }
            if hasattr(self, 'wallet') and self.wallet and hasattr(self.wallet, 'address'):
                payload['address'] = self.wallet.address
            
            if searchText:
                payload['search'] = searchText

            response = self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/streams/search/paginated',
                payload=json.dumps(payload),
            )
            response_data = response.json()
            
            if isinstance(response_data, dict):
                if 'streams' in response_data and 'pagination' in response_data:
                    streams = cleanAndSort(response_data['streams'], searchText)
                    pagination = response_data['pagination']
                    total_count = pagination.get('total_count', len(streams))
                    return streams, total_count
                
                elif 'streams' in response_data:
                    streams = cleanAndSort(response_data['streams'], searchText)
                    total_count = len(streams)  # Fallback
                    return streams, total_count
                    
            elif isinstance(response_data, list):
                streams = cleanAndSort(response_data, searchText)
                total_count = len(streams)
                return streams, total_count
                
            else:
                logging.warning(f"Unexpected response format: {type(response_data)}")
                return [], 0
                    
        except Exception as e:
            logging.error(f"Error in getSearchStreamsPaginated: {str(e)}")
            return [], 0

    def getSearchPredictionStreamsPaginated(self, searchText: str = None, page: int = 1, per_page: int = 100, 
                            sort_by: str = 'popularity', order: str = 'desc') -> tuple[list, int]:
        """ Get prediction streams with full pagination information """
        def cleanAndSort(streams: list, searchText: str = None):
            """Clean and sanitize stream data"""
            return sanitizeJson(streams)

        try:
            page = max(1, page)
            per_page = min(max(1, per_page), 200)
            payload = {
                'page': page,
                'per_page': per_page,
                'sort': sort_by,
                'order': order
            }
            if hasattr(self, 'wallet') and self.wallet and hasattr(self.wallet, 'address'):
                payload['address'] = self.wallet.address
            
            if searchText:
                payload['search'] = searchText

            response = self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/streams/search/prediction/paginated',
                payload=json.dumps(payload),
            )
            response_data = response.json()
            
            if isinstance(response_data, dict):
                if 'streams' in response_data and 'pagination' in response_data:
                    streams = cleanAndSort(response_data['streams'], searchText)
                    pagination = response_data['pagination']
                    total_count = pagination.get('total_count', len(streams))
                    return streams, total_count
                
                elif 'streams' in response_data:
                    streams = cleanAndSort(response_data['streams'], searchText)
                    total_count = len(streams)  # Fallback
                    return streams, total_count
                    
            elif isinstance(response_data, list):
                streams = cleanAndSort(response_data, searchText)
                total_count = len(streams)
                return streams, total_count
                
            else:
                logging.warning(f"Unexpected response format: {type(response_data)}")
                return [], 0
                    
        except Exception as e:
            logging.error(f"Error in getSearchPredictionStreamsPaginated: {str(e)}")
            return [], 0

    def marketStreamsSetPrice(self, streamUuid: str = None, pricePerObs: float = None) -> bool:
        """
        Set the price per observation for a stream.
        
        Args:
            streamUuid: A StreamUuid we wish to set the price for
            pricePerObs: The price per observation we wish to set
            
        Returns:
            bool: True if the price per observation request was successful, False otherwise
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/market/streams/set/price',
                payload=json.dumps({
                    'streamUuid': streamUuid,
                    'pricePerObs': pricePerObs}))
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error setting price per observation: {str(e)}")
            return False

    def getCentrifugoToken(self) -> dict:
        """
        Get the centrifugo token for the user.
        
        Returns: {
            "token": token,
            "ws_url": CENTRIFUGO_WS_URL,
            "expires_at": expires_at.isoformat() + "Z",
            "user_id": user_id}
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/centrifugo/token')
            if response.status_code == 200:
                return response.json()
            else:
                return {}
        except Exception as e:
            logging.error(f"Error setting price per observation: {str(e)}")
            return False

    def marketBuyStream(self, streamUuid: str = None) -> bool:
        """
        Buy a stream by sending a request to the server.
        
        Args:
            streamUuid: A StreamUuid we wish to buy
            
        Returns:
            bool: True if the buy request was successful, False otherwise
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/market/streams/buy',
                payload=json.dumps({'streamUuid': streamUuid}))
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error predicting stream: {str(e)}")
            return False

    def incrementVote(self, streamId: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/vote_on/sanction/incremental',
            payload=json.dumps({'streamId': streamId})).text

    def removeVote(self, streamId: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/clear_vote_on/sanction/incremental',
            payload=json.dumps({'streamId': streamId})).text

    def predictStream(self, streamId: int) -> bool:
        """
        Start predicting a stream by sending a request to the server.
        
        Args:
            streamId: A StreamId object containing the stream details to predict
            
        Returns:
            bool: True if the prediction request was successful, False otherwise
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/request/stream/specific',
                payload=json.dumps({'streamId': streamId}))
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error predicting stream: {str(e)}")
            return False

    def flagStream(self, streamId: int) -> bool:
        """
        Flag a stream as inappropriate or bad by sending a request to the server.
        Args:
            streamId: The stream ID to flag
        Returns:
            bool: True if the flag request was successful, False otherwise
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/flag/stream',
                payload=json.dumps({'streamId': streamId}))
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error flagging stream: {str(e)}")
            return False

    def getObservations(self, streamId: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/observations/list',
            payload=json.dumps({'streamId': streamId})).text

    def getPowerBalance(self):
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/api/v0/balance/get').text
    
    def submitMaifestVote(self, wallet: Wallet, votes: dict[str, int]):
        # todo authenticate the vault instead
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/vote_on/manifest',
            useWallet=wallet,
            payload=json.dumps(votes or {})).text

    def submitSanctionVote(self, wallet: Wallet, votes: dict[str, int]):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/vote_on/sanction',
            useWallet=wallet,
            payload=json.dumps(votes or {})).text

    def removeSanctionVote(self, wallet: Wallet):
        return self._makeAuthenticatedCall(
            function=requests.Get,
            endpoint='/clear_votes_on/sanction',
            useWallet=wallet).text

    def poolParticipants(self, vaultAddress: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/pool/participants',
            payload=json.dumps({'vaultAddress': vaultAddress})).text

    def pinDepinStream(self, stream: dict = None) -> tuple[bool, str]:
        ''' removes a stream from the server '''
        if stream is None:
            raise ValueError('stream must be provided')
        response = self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/subscription/pindepin',
            payload=json.dumps(stream))
        if response.status_code < 400:
            return response.json().get('success'), response.json().get('result')
        return False, ''

    def mineToAddressStatus(self) -> Union[str, None]:
        ''' get reward address '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/mine/to/address')
            if response.status_code > 399:
                return 'Unknown'
            if response.text in ['null', 'None', 'NULL']:
                return ''
            return response.text
        except Exception as e:
            logging.warning(
                'unable to get reward address; try again Later.', e, color='yellow')
            return None
        return None

    def setRewardAddress(
        self,
        signature: Union[str, bytes],
        pubkey: str,
        address: str,
        usingVault: bool = False,
    ) -> tuple[bool, str]:
        ''' just like mine to address but using the wallet '''
        try:
            if isinstance(signature, bytes):
                signature = signature.decode()
            if usingVault:
                js = json.dumps({
                    'vaultSignature': signature,
                    'vaultPubkey': pubkey,
                    'address': address})
            else:
                js = json.dumps({
                    'signature': signature,
                    'pubkey': pubkey,
                    'address': address})
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/mine/to/address',
                payload=js)
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to set reward address; try again Later.', e, color='yellow')
            return False, ''

    def stakeForAddress(
        self,
        vaultSignature: Union[str, bytes],
        vaultPubkey: str,
        address: str
    ) -> tuple[bool, str]:
        ''' add stake address '''
        try:
            if isinstance(vaultSignature, bytes):
                vaultSignature = vaultSignature.decode()
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/stake/for/address',
                raiseForStatus=False,
                payload=json.dumps({
                    'vaultSignature': vaultSignature,
                    'vaultPubkey': vaultPubkey,
                    'address': address}))
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to determine status of mine to address feature due to connection timeout; try again Later.', e, color='yellow')
            return False, ''

    def lendToAddress(
        self,
        vaultSignature: Union[str, bytes],
        vaultPubkey: str,
        address: str,
        vaultAddress: str = '',
    ) -> tuple[bool, str]:
        ''' add lend address '''
        try:
            if isinstance(vaultSignature, bytes):
                vaultSignature = vaultSignature.decode()
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/stake/lend/to/address',
                raiseForStatus=False,
                payload=json.dumps({
                    'vaultSignature': vaultSignature,
                    'vaultAddress': vaultAddress,
                    'vaultPubkey': vaultPubkey,
                    'address': address}))
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to determine status of mine to address feature due to connection timeout; try again Later.', e, color='yellow')
            return False, ''

    def lendRemove(self) -> tuple[bool, dict]:
        ''' removes a stream from the server '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/lend/remove')
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to stakeProxyRemove due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def lendAddress(self) -> Union[str, None]:
        ''' get lending address '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/lend/address')
            if response.status_code > 399:
                return 'Unknown'
            if response.text in ['null', 'None', 'NULL']:
                return ''
            return response.text
        except Exception as e:
            logging.warning(
                'unable to get reward address; try again Later.', e, color='yellow')
            return ''

    def registerVault(
        self,
        walletSignature: Union[str, bytes],
        vaultSignature: Union[str, bytes],
        vaultPubkey: str,
        address: str,
    ) -> tuple[bool, str]:
        ''' removes a stream from the server '''
        if isinstance(walletSignature, bytes):
            walletSignature = walletSignature.decode()
        if isinstance(vaultSignature, bytes):
            vaultSignature = vaultSignature.decode()
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/register/vault',
                payload=json.dumps({
                    'walletSignature': walletSignature,
                    'vaultSignature': vaultSignature,
                    'vaultPubkey': vaultPubkey,
                    'address': address}))
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to register vault address due to connection timeout; try again Later.', e, color='yellow')
            return False, ''


    def fetchWalletStatsDaily(self) -> str:
        ''' gets wallet stats '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/wallet/stats/daily')
            return response.json()
        except Exception as e:
            logging.warning(
                'unable to disable status of Mine-To-Vault feature due to connection timeout; try again Later.', e, color='yellow')
            return ''

    def stakeCheck(self) -> bool:
        ''' gets wallet stats '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/check')
            if response.text == 'TRUE':
                return True
        except Exception as e:
            logging.warning(
                'unable to disable status of Mine-To-Vault feature due to connection timeout; try again Later.', e, color='yellow')
            return False
        return False

    def setEthAddress(self, ethAddress: str) -> tuple[bool, dict]:
        ''' removes a stream from the server '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/set/eth/address',
                payload=json.dumps({'ethaddress': ethAddress}))
            return response.status_code < 400, response.json()
        except Exception as e:
            logging.warning(
                'unable to claim beta due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def poolAddresses(self) -> tuple[bool, dict]:
        ''' removes a stream from the server '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/lend/addresses')
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to stakeProxyRequest due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def poolAddressRemove(self, lend_id: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/stake/lend/address/remove',
            payload=json.dumps({'lend_id': lend_id})).text

    def stakeProxyChildren(self) -> tuple[bool, dict]:
        ''' removes a stream from the server '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/proxy/children')
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to stakeProxyRequest due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def stakeProxyCharity(self, address: str, childId: int) -> tuple[bool, dict]:
        ''' charity for stake '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/stake/proxy/charity',
                payload=json.dumps({
                    'child': address,
                    **({} if childId in [None, 0, '0'] else {'childId': childId})
                }))
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to stakeProxyCharity due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def stakeProxyCharityNot(self, address: str, childId: int) -> tuple[bool, dict]:
        ''' no charity for stake '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/stake/proxy/charity/not',
                payload=json.dumps({
                    'child': address,
                    **({} if childId in [None, 0, '0'] else {'childId': childId})
                }))
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to stakeProxyCharityNot due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def delegateGet(self) -> tuple[bool, str]:
        ''' my delegate '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/proxy/delegate')
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to delegateGet due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def delegateRemove(self) -> tuple[bool, str]:
        ''' my delegate '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/proxy/delegate/remove')
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to delegateRemove due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def stakeProxyRemove(self, address: str, childId: int) -> tuple[bool, dict]:
        ''' removes a stream from the server '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/stake/proxy/remove',
                payload=json.dumps({'child': address, 'childId': childId}))
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to stakeProxyRemove due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def invitedBy(self, address: str) -> tuple[bool, dict]:
        ''' removes a stream from the server '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/invited/by',
                payload=json.dumps({'referrer': address}))
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to report referrer due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def publish(
        self,
        topic: str,
        data: str,
        observationTime: str,
        observationHash: str,
        isPrediction: bool = True,
        useAuthorizedCall: bool = True,
    ) -> Union[bool, None]:
        ''' publish predictions '''
        #logging.info(f'publishing', color='blue')
        # if not isPrediction and self.topicTime.get(topic, 0) > time.time() - (Stream.minimumCadence*.95):
        #    return
        # if isPrediction and self.topicTime.get(topic, 0) > time.time() - 60*60:
        #    return
        if self.topicTime.get(topic, 0) > time.time() - (Stream.minimumCadence*.95):
            return
        self.setTopicTime(topic)
        try:
            if useAuthorizedCall:
                response = self._makeAuthenticatedCall(
                    function=requests.post,
                    endpoint='/record/prediction/authed' if isPrediction else '/record/observation/authed',
                    payload=json.dumps({
                        'topic': topic,
                        'data': str(data),
                        'time': str(observationTime),
                        'hash': str(observationHash),
                    }))
            else:
                response = self._makeUnauthenticatedCall(
                    function=requests.post,
                    endpoint='/record/prediction' if isPrediction else '/record/observation',
                    payload=json.dumps({
                        'topic': topic,
                        'data': str(data),
                        'time': str(observationTime),
                        'hash': str(observationHash),
                    }))
            # response = self._makeAuthenticatedCall(
            #    function=requests.get,
            #    endpoint='/record/prediction')
            if response.status_code == 200:
                return True
            if response.status_code > 399:
                return None
            if response.text.lower() in ['fail', 'null', 'none', 'error']:
                return False
        except Exception as _:
            # logging.warning(
            #    'unable to determine if prediction was accepted; try again Later.', e, color='yellow')
            return None
        return True

    # def getProposalById(self, proposal_id: str) -> dict:
    #    try:
    #        response = self._makeUnauthenticatedCall(
    #            function=requests.get,
    #            endpoint=f'/proposals/get/{proposal_id}'  # Update endpoint path
    #        )
    #        if response.status_code == 200:
    #            return response.json()
    #        else:
    #            logging.error(f"Failed to get proposal. Status code: {response.status_code}")
    #            return None
    #    except Exception as e:
    #        logging.error(f"Error occurred while fetching proposal: {str(e)}")
    #        return None

    def getProposals(self):
        """
        Function to get all proposals by calling the API endpoint.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/get/all'
            )
            if response.status_code == 200:
                proposals = response.json()
                return proposals
            else:
                logging.error(
                    f"Failed to get proposals. Status code: {response.status_code}", color='red')
                return []
        except requests.RequestException as e:
            logging.error(
                f"Error occurred while fetching proposals: {str(e)}", color='red')
            return []

    def getApprovedProposals(self):
        """
        Function to get all approved proposals by calling the API endpoint.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/get/approved'
            )
            if response.status_code == 200:
                proposals = response.json()
                return proposals
            else:
                logging.error(
                    f"Failed to get approved proposals. Status code: {response.status_code}", color='red')
                return []
        except requests.RequestException as e:
            logging.error(
                f"Error occurred while fetching approved proposals: {str(e)}", color='red')
            return []

    def submitProposal(self, proposal_data: dict) -> tuple[bool, dict]:
        '''submits proposal'''
        try:
            # Ensure options is a JSON string
            if 'options' in proposal_data and isinstance(proposal_data['options'], list):
                proposal_data['options'] = json.dumps(proposal_data['options'])

            # Convert the entire proposal_data to a JSON string
            proposal_json_string = json.dumps(proposal_data)

            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/proposal/submit',
                payload=proposal_json_string
            )
            if response.status_code < 400:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                logging.error(f"Error in submitProposal: {error_message}")
                return False, {"error": error_message}

        except RequestException as re:
            error_message = f"Request error in submitProposal: {str(re)}"
            logging.error(error_message)
            logging.error(traceback.format_exc())
            return False, {"error": error_message}
        except Exception as e:
            error_message = f"Unexpected error in submitProposal: {str(e)}"
            logging.error(error_message)
            logging.error(traceback.format_exc())
            return False, {"error": error_message}

    def getProposalById(self, proposal_id: str) -> dict:
        """
        Function to get a specific proposal by ID by calling the API endpoint.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=f'/proposal/{proposal_id}'
            )
            if response.status_code == 200:
                return response.json()['proposal']
            else:
                logging.error(
                    f"Failed to get proposal. Status code: {response.status_code}",
                    extra={'color': 'red'}
                )
                return None
        except requests.RequestException as e:
            logging.error(
                f"Error occurred while fetching proposal: {str(e)}",
                extra={'color': 'red'}
            )
            return None

    def getProposalVotes(self, proposal_id: str, format_type: str = None) -> dict:
        '''Gets proposal votes with option for raw or processed format'''
        try:
            endpoint = f'/proposal/votes/get/{proposal_id}'
            if format_type:
                endpoint += f'?format={format_type}'

            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=endpoint
            )

            if response.status_code == 200:
                return response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return {'status': 'error', 'message': error_message}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def getExpiredProposals(self) -> dict:
        """
        Fetches expired proposals
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/expired'
            )
            if response.status_code == 200:
                return {'status': 'success', 'proposals': response.json()}
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return {'status': 'error', 'message': error_message}
        except Exception as e:
            error_message = f"Error in getExpiredProposals: {str(e)}"
            return {'status': 'error', 'message': error_message}

    def isApprovedAdmin(self, address: str) -> bool:
        """Check if a wallet address has admin rights"""
        if address not in {
            "ES48mkqM5wMjoaZZLyezfrMXowWuhZ8u66",
            "Efnsr27fc276Wp7hbAqZ5uo7Rn4ybrUqmi",
            "EQGB7cBW3HvafARDoYsgceJS2W7ZhKe3b6",
            "EHkDUkADkYnUY1cjCa5Lgc9qxLTMUQEBQm",
        }:
            return False
        response = self._makeUnauthenticatedCall(
            function=requests.get,
            endpoint='/proposals/admin')
        if response.status_code == 200:
            return address in response.json()
        return False

    def getUnapprovedProposals(self, address: str = None) -> dict:
        """Get unapproved proposals only if user has admin rights"""
        try:
            if not self.isApprovedAdmin(address):
                return {
                    'status': 'error',
                    'message': 'Unauthorized access'
                }

            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/unapproved'
            )

            if response.status_code == 200:
                return {
                    'status': 'success',
                    'proposals': response.json()
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Failed to fetch unapproved proposals'
                }

        except Exception as e:
            logging.error(f"Error in getUnapprovedProposals: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }

    def approveProposal(self, address: str, proposal_id: int) -> tuple[bool, dict]:
        """Approve a proposal only if user has admin rights"""
        try:
            if not self.isApprovedAdmin(address):
                return False, {'error': 'Unauthorized access'}

            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint=f'/proposals/approve/{proposal_id}'
            )

            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {'error': f"Failed to approve proposal: {response.text}"}

        except Exception as e:
            return False, {'error': str(e)}

    def disapproveProposal(self, address: str, proposal_id: int) -> tuple[bool, dict]:
        """Disapprove a proposal only if user has admin rights"""
        try:
            if not self.isApprovedAdmin(address):
                return False, {'error': 'Unauthorized access'}

            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint=f'/proposals/disapprove/{proposal_id}'
            )

            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {'error': f"Failed to disapprove proposal: {response.text}"}

        except Exception as e:
            return False, {'error': str(e)}

    def getActiveProposals(self) -> dict:
        """
        Fetches active proposals
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/active'
            )
            if response.status_code == 200:
                return {'status': 'success', 'proposals': response.json()}
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return {'status': 'error', 'message': error_message}
        except Exception as e:
            error_message = f"Error in getActiveProposals: {str(e)}"
            return {'status': 'error', 'message': error_message}

    def submitProposalVote(self, proposal_id: int, vote: str) -> tuple[bool, dict]:
        """
        Submits a vote for a proposal
        """
        try:
            vote_data = {
                "proposal_id": int(proposal_id),  # Send proposal_id as integer
                "vote": str(vote),
            }
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/proposal/vote/submit',
                payload=vote_data  # Pass the vote_data dictionary directly
            )
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}

        except Exception as e:
            error_message = f"Error in submitProposalVote: {str(e)}"
            return False, {"error": error_message}

    def poolAccepting(self, status: bool) -> tuple[bool, dict]:
        """
        Function to set the pool status to accepting or not accepting
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/lend/enable' if status else '/stake/lend/disable')
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in poolAccepting: {str(e)}"
            return False, {"error": error_message}

    ## untested ##

    def setPoolSize(self, poolStakeLimit: float) -> tuple[bool, dict]:
        """
        Function to set the pool size
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/pool/size/set',
                payload=json.dumps({"poolStakeLimit": float(poolStakeLimit)}))
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in poolAcceptingWorkers: {str(e)}"
            return False, {"error": error_message}

    def setPoolWorkerReward(self, rewardPercentage: float) -> tuple[bool, dict]:
        """
        Function to set the pool status to accepting or not accepting
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/pool/worker/reward/set',
                payload=json.dumps({"rewardPercentage": float(rewardPercentage)}))
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in poolAcceptingWorkers: {str(e)}"
            return False, {"error": error_message}

    def getPoolSize(self, address: str) -> tuple[bool, dict]:
        """
        Function to set the pool status to accepting or not accepting
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=f'/pool/size/get/{address}')
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in poolAcceptingWorkers: {str(e)}"
            return False, {"error": error_message}

    def getPoolWorkerReward(self, address: str) -> tuple[bool, dict]:
        """
        Function to set the pool status to accepting or not accepting
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=f'/pool/worker/reward/get/{address}')
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in poolAcceptingWorkers: {str(e)}"
            return False, {"error": error_message}

    def setMiningMode(self, status: bool) -> tuple[bool, dict]:
        """
        Set the worker mining mode.

        In P2P mode, stores preference locally.
        In hybrid/central mode, syncs with central server.
        """
        mode = _get_networking_mode()

        # In P2P mode, store locally (mining mode is a local preference)
        if mode == 'p2p':
            self._mining_mode = status
            return True, {'mining_mode': status, 'stored': 'local'}

        # Hybrid/central: sync with server
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/worker/mining/mode/enable' if status else '/worker/mining/mode/disable')
            if response.status_code == 200:
                self._mining_mode = status
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            # In hybrid mode, store locally as fallback
            if mode == 'hybrid':
                self._mining_mode = status
                return True, {'mining_mode': status, 'stored': 'local_fallback'}
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getMiningMode(self, address) -> tuple[bool, dict]:
        """
        Function to set the worker mining mode
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=f'/worker/mining/mode/get/{address}')
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def loopbackCheck(self, ipAddress:Union[str, None], port: Union[int, None]) -> bool:
        """
        Check if our data server is publicly reachable.

        In P2P mode, uses libp2p's NAT traversal (AutoNAT).
        In hybrid/central mode, asks central server.
        """
        mode = _get_networking_mode()

        # In P2P mode, use libp2p's NAT detection if available
        if mode == 'p2p' and self._p2p_client:
            try:
                client = self._p2p_client
                # Check if we're publicly reachable via libp2p
                if hasattr(client, 'is_publicly_reachable'):
                    return client.is_publicly_reachable()
                # Fallback: assume we're reachable if we have external addresses
                if hasattr(client, 'get_external_addresses'):
                    addrs = client.get_external_addresses()
                    return len(addrs) > 0
            except Exception as e:
                logging.warning(f"P2P loopback check failed: {e}")
            # In pure P2P mode, assume reachable (libp2p handles NAT traversal)
            return True

        # Hybrid/central: ask central server
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/loopback/check',
                payload=json.dumps({
                    **({'ip': str(ipAddress)} if ipAddress is not None else {}),
                    **({'port': port} if port is not None else {})}))
            if response.status_code == 200:
                try:
                    return response.json().get('port_open', False)
                except Exception as e:
                    return False
            else:
                return False
        except Exception as e:
            logging.warning(f"Loopback check failed: {e}")
            return False

    def getSubscribers(self) -> tuple[bool, list]:
        """
        asks the central server (could ask fellow Neurons) if our own dataserver
        is publically reachable.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/get/subscribers')
            if response.status_code == 200:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getStreamsSubscribers(self, streams:list[str]) -> tuple[bool, list]:
        """
        asks the central server (could ask fellow Neurons) if our own dataserver
        is publically reachable.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/get/stream/subscribers',
                payload=json.dumps({'streams': streams}))
            if 200 <= response.status_code < 400:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getStreamsPublishers(self, streams:list[str]) -> tuple[bool, list]:
        """
        asks the central server (could ask fellow Neurons) if our own dataserver
        is publically reachable.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/get/stream/publisher',
                payload=json.dumps({'streams': streams}))
            if 200 <= response.status_code < 400:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}


    def getDataManagerPort(self) -> tuple[bool, list]:
        """
        gets the datamanager port for a wallet
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.t,
                endpoint='/api/v0/datamanager/port/get')
            if 200 <= response.status_code < 400:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getDataManagerPortByAddress(self, address:str) -> tuple[bool, list]:
        """
        gets the datamanager port for a wallet
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/datamanager/port/get/{address}')
            if 200 <= response.status_code < 400:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def setDataManagerPort(self, port: int) -> tuple[bool, list]:
        """
        Set the data manager port.

        In P2P mode, stores locally and updates peer announcement.
        In hybrid/central mode, syncs with central server.
        """
        mode = _get_networking_mode()

        # In P2P mode, store locally and update peer announcement
        if mode == 'p2p':
            self._data_manager_port = port
            # Update P2P announcement if available
            if self._p2p_client:
                try:
                    client = self._p2p_client
                    if hasattr(client, 'update_announcement'):
                        client.update_announcement(data_port=port)
                except Exception as e:
                    logging.warning(f"Failed to update P2P announcement: {e}")
            return True, {'port': port, 'stored': 'local'}

        # Hybrid/central: sync with server
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint=f'/api/v0/datamanager/port/set/{port}')
            if 200 <= response.status_code < 400:
                self._data_manager_port = port
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            # In hybrid mode, store locally as fallback
            if mode == 'hybrid':
                self._data_manager_port = port
                return True, {'port': port, 'stored': 'local_fallback'}
            error_message = f"Error in setDataManagerPort: {str(e)}"
            return False, {"error": error_message}


    def getContentCreated(self) -> tuple[bool, dict]:
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/content/created/get')
            if response.status_code == 200:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in getContentCreated: {str(e)}"
            return False, {"error": error_message}

    def approveInviters(self, approved: list[int]) -> tuple[bool, list]:
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/inviters/approve',
                payload=json.dumps({"approved": approved}))
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            return False, {"error": f"Error in setMiningMode: {str(e)}"}

    def disapproveInviters(self, disapproved: list[int]) -> tuple[bool, dict]:
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/inviters/disapprove',
                payload=json.dumps({"disapproved": disapproved}))
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def deleteContent(self, deleted: list[int]) -> tuple[bool, str]:
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/content/delete',
                payload=json.dumps({"deleted": deleted}))
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getBalances(self) -> tuple[bool, dict]:
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/balances/get')
            if response.status_code == 200:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    # ========================================================================
    # DONATION API METHODS (Central fallback for P2P donation system)
    # ========================================================================

    def getTreasuryAddress(self) -> tuple[bool, dict]:
        """Get the multi-sig treasury address for donations."""
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/donation/treasury-address')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return False, {"error": str(e)}

    def submitDonation(
        self,
        amount: float,
        tx_hash: str,
        donor_address: str,
    ) -> tuple[bool, dict]:
        """
        Submit a donation record to the central server.

        This is called after the EVR transaction is broadcast to record
        the donation for reward tracking.

        Args:
            amount: Amount of EVR donated
            tx_hash: Evrmore transaction hash
            donor_address: Donor's wallet address

        Returns:
            Tuple of (success, response_data)
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/donation/submit',
                payload=json.dumps({
                    'amount': amount,
                    'tx_hash': tx_hash,
                    'donor_address': donor_address,
                }))
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return False, {"error": str(e)}

    def getDonorStats(self, donor_address: str = None) -> tuple[bool, dict]:
        """
        Get donation statistics for a donor.

        Args:
            donor_address: Address to query (None = authenticated wallet)

        Returns:
            Tuple of (success, stats_dict) where stats_dict contains:
            {
                'total_donated': float,
                'tier': str,
                'next_tier': str,
                'next_tier_threshold': float,
                'progress_to_next': float,
                'rewards_received': float,
                'donation_count': int,
                'first_donation_ts': int,
                'latest_donation_ts': int,
                'tier_badges': list[str],
            }
        """
        try:
            endpoint = '/api/v0/donation/stats'
            if donor_address:
                endpoint += f'/{donor_address}'
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint=endpoint)
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return False, {"error": str(e)}

    def getDonationHistory(
        self,
        donor_address: str = None,
        limit: int = 50,
        offset: int = 0
    ) -> tuple[bool, list]:
        """
        Get donation history for a donor.

        Args:
            donor_address: Address to query (None = authenticated wallet)
            limit: Max records to return
            offset: Pagination offset

        Returns:
            Tuple of (success, list of donation records)
        """
        try:
            endpoint = f'/api/v0/donation/history?limit={limit}&offset={offset}'
            if donor_address:
                endpoint += f'&address={donor_address}'
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint=endpoint)
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, []
        except Exception as e:
            logging.warning(f"Failed to get donation history: {e}")
            return False, []

    def getTopDonors(self, limit: int = 20) -> tuple[bool, list]:
        """
        Get top donors leaderboard.

        Args:
            limit: Max donors to return

        Returns:
            Tuple of (success, list of donor stats)
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=f'/api/v0/donation/top-donors?limit={limit}')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, []
        except Exception as e:
            logging.warning(f"Failed to get top donors: {e}")
            return False, []

    def getTierAchievements(self, tier: str = None) -> tuple[bool, list]:
        """
        Get tier achievements (first-to-tier unique badges).

        Args:
            tier: Optional filter by tier name

        Returns:
            Tuple of (success, list of achievement records)
        """
        try:
            endpoint = '/api/v0/donation/achievements'
            if tier:
                endpoint += f'?tier={tier}'
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=endpoint)
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, []
        except Exception as e:
            logging.warning(f"Failed to get tier achievements: {e}")
            return False, []

    def getDonationRate(self) -> tuple[bool, dict]:
        """
        Get current EVR to SATORI donation exchange rate.

        Returns:
            Tuple of (success, rate_dict) where rate_dict contains:
            {
                'evr_to_satori': float,
                'min_donation': float,
                'max_donation': float,
                'updated_at': int,
            }
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/donation/rate')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {"evr_to_satori": 0.01, "min_donation": 1, "max_donation": 1000000}
        except Exception as e:
            logging.warning(f"Failed to get donation rate: {e}")
            return False, {"evr_to_satori": 0.01, "min_donation": 1, "max_donation": 1000000}

    # ========================================================================
    # SIGNER API METHODS (For authorized multi-sig signers)
    # ========================================================================

    def isAuthorizedSigner(self, address: str = None) -> bool:
        """
        Check if an address is an authorized signer.

        Args:
            address: Address to check (None = authenticated wallet)

        Returns:
            True if authorized signer
        """
        try:
            endpoint = '/api/v0/signer/check'
            if address:
                endpoint += f'/{address}'
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint=endpoint)
            if response.status_code == 200:
                return response.json().get('is_signer', False)
            return False
        except Exception as e:
            logging.warning(f"Failed to check signer status: {e}")
            return False

    def getSignerPendingRequests(self) -> tuple[bool, list]:
        """
        Get pending signature requests for authorized signers.

        Returns:
            Tuple of (success, list of pending requests)
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/signer/pending')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, []
        except Exception as e:
            logging.warning(f"Failed to get pending requests: {e}")
            return False, []

    def approveSignerRequest(self, request_id: str, signature: str = None) -> tuple[bool, dict]:
        """
        Approve and sign a pending request.

        Args:
            request_id: ID of request to approve
            signature: Pre-computed signature (None = sign with wallet)

        Returns:
            Tuple of (success, response_dict)
        """
        try:
            payload = {'request_id': request_id}
            if signature:
                payload['signature'] = signature
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/signer/approve',
                payload=json.dumps(payload))
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return False, {"error": str(e)}

    def rejectSignerRequest(self, request_id: str, reason: str = None) -> tuple[bool, dict]:
        """
        Reject a pending request.

        Args:
            request_id: ID of request to reject
            reason: Optional rejection reason

        Returns:
            Tuple of (success, response_dict)
        """
        try:
            payload = {'request_id': request_id}
            if reason:
                payload['reason'] = reason
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/signer/reject',
                payload=json.dumps(payload))
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return False, {"error": str(e)}

    def getSignerRequestStatus(self, request_id: str) -> tuple[bool, dict]:
        """
        Get status of a signing request.

        Args:
            request_id: ID of request

        Returns:
            Tuple of (success, status_dict)
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint=f'/api/v0/signer/request/{request_id}')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {"error": f"Status {response.status_code}: {response.text}"}
        except Exception as e:
            return False, {"error": str(e)}

    def getTreasuryBalance(self) -> tuple[bool, dict]:
        """
        Get treasury balance (for signer dashboard).

        Returns:
            Tuple of (success, balance_dict) with EVR and SATORI balances
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/signer/treasury-balance')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {"evr": 0, "satori": 0}
        except Exception as e:
            logging.warning(f"Failed to get treasury balance: {e}")
            return False, {"evr": 0, "satori": 0}

    # =========================================================================
    # TREASURY ALERTS & DEFERRED REWARDS
    # =========================================================================

    def getTreasuryAlertStatus(self) -> tuple[bool, dict]:
        """
        Get current treasury status and active alerts.

        Returns:
            Tuple of (success, status_dict) with:
            - severity: 'info'|'warning'|'critical'
            - satori_level: Treasury SATORI status
            - evr_level: Treasury EVR status
            - active_alert: Current active alert if any
        """
        mode = _get_networking_mode()

        # Try P2P first in hybrid/p2p mode
        if mode in ('hybrid', 'p2p') and self._p2p_client:
            try:
                client = self._p2p_client
                if client and client.get('alert_manager'):
                    manager = client['alert_manager']
                    if hasattr(manager, 'get_current_status'):
                        status = manager.get_current_status()
                        if status:
                            result = {
                                'severity': status.severity,
                                'satori_level': status.satori_level,
                                'evr_level': status.evr_level,
                                'satori_balance': getattr(status, 'satori_balance', None),
                                'evr_balance': getattr(status, 'evr_balance', None),
                                'active_alert': None,
                            }
                            if hasattr(manager, 'get_active_alert'):
                                alert = manager.get_active_alert()
                                if alert:
                                    result['active_alert'] = {
                                        'type': alert.alert_type,
                                        'severity': alert.severity,
                                        'message': alert.message,
                                        'timestamp': alert.timestamp,
                                    }
                            return True, result
            except Exception as e:
                logging.warning(f"P2P treasury alert status failed: {e}")
                if mode == 'p2p':
                    return False, {'error': str(e)}

        # Central fallback
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/treasury/status')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {'severity': 'info', 'satori_level': 'Unknown', 'evr_level': 'Unknown'}
        except Exception as e:
            logging.warning(f"Failed to get treasury alert status: {e}")
            return False, {'severity': 'info', 'satori_level': 'Unknown', 'evr_level': 'Unknown'}

    def getDeferredRewards(self, address: str = None) -> tuple[bool, dict]:
        """
        Get deferred rewards for an address.

        Args:
            address: Wallet address (uses self.wallet if None)

        Returns:
            Tuple of (success, summary_dict) with:
            - total_pending: Total deferred amount
            - deferred_count: Number of deferred rewards
            - deferred_rewards: List of individual deferred rewards
        """
        address = address or self.wallet.address
        mode = _get_networking_mode()

        # Try P2P first in hybrid/p2p mode
        if mode in ('hybrid', 'p2p') and self._p2p_client:
            try:
                client = self._p2p_client
                if client and client.get('deferred_rewards_manager'):
                    manager = client['deferred_rewards_manager']
                    if hasattr(manager, 'get_deferred_for_address'):
                        summary = manager.get_deferred_for_address(address)
                        if summary:
                            return True, {
                                'total_pending': summary.total_pending,
                                'deferred_count': summary.deferred_count,
                                'oldest_deferred_at': summary.oldest_deferred_at,
                                'newest_deferred_at': summary.newest_deferred_at,
                                'deferred_rewards': [
                                    {
                                        'round_id': r.round_id,
                                        'amount': r.amount,
                                        'reason': r.reason,
                                        'created_at': r.created_at,
                                    }
                                    for r in summary.deferred_rewards[:20]
                                ],
                            }
            except Exception as e:
                logging.warning(f"P2P deferred rewards failed: {e}")
                if mode == 'p2p':
                    return False, {'error': str(e)}

        # Central fallback
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint=f'/api/v0/treasury/deferred/{address}')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {'total_pending': 0, 'deferred_count': 0, 'deferred_rewards': []}
        except Exception as e:
            logging.warning(f"Failed to get deferred rewards: {e}")
            return False, {'total_pending': 0, 'deferred_count': 0, 'deferred_rewards': []}

    def getTreasuryAlertHistory(self, limit: int = 20) -> tuple[bool, list]:
        """
        Get recent treasury alert history.

        Args:
            limit: Maximum number of alerts to return

        Returns:
            Tuple of (success, alert_list)
        """
        mode = _get_networking_mode()

        # Try P2P first in hybrid/p2p mode
        if mode in ('hybrid', 'p2p') and self._p2p_client:
            try:
                client = self._p2p_client
                if client and client.get('alert_manager'):
                    manager = client['alert_manager']
                    if hasattr(manager, 'get_alert_history'):
                        history = manager.get_alert_history(limit=limit)
                        return True, [
                            {
                                'type': a.alert_type,
                                'severity': a.severity,
                                'message': a.message,
                                'timestamp': a.timestamp,
                                'resolved': getattr(a, 'resolved', False),
                                'resolved_at': getattr(a, 'resolved_at', None),
                            }
                            for a in history
                        ]
            except Exception as e:
                logging.warning(f"P2P alert history failed: {e}")
                if mode == 'p2p':
                    return False, []

        # Central fallback
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=f'/api/v0/treasury/alerts/history?limit={limit}')
            if response.status_code == 200:
                data = response.json()
                return True, data.get('history', [])
            else:
                return False, []
        except Exception as e:
            logging.warning(f"Failed to get alert history: {e}")
            return False, []

    def getTotalDeferredRewards(self) -> tuple[bool, dict]:
        """
        Get total deferred rewards across all users (for signer dashboard).

        Returns:
            Tuple of (success, stats_dict) with aggregate deferred stats
        """
        mode = _get_networking_mode()

        # Try P2P first in hybrid/p2p mode
        if mode in ('hybrid', 'p2p') and self._p2p_client:
            try:
                client = self._p2p_client
                if client and client.get('deferred_rewards_manager'):
                    manager = client['deferred_rewards_manager']
                    if hasattr(manager, 'get_stats'):
                        stats = manager.get_stats()
                        return True, stats
            except Exception as e:
                logging.warning(f"P2P total deferred failed: {e}")
                if mode == 'p2p':
                    return False, {'error': str(e)}

        # Central fallback
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/treasury/deferred/total')
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, {'total_deferred': 0, 'deferred_count': 0}
        except Exception as e:
            logging.warning(f"Failed to get total deferred: {e}")
            return False, {'total_deferred': 0, 'deferred_count': 0}

    def set_alert_manager(self, manager):
        """Set the treasury alert manager for P2P mode."""
        client = self._get_p2p_client()
        if client:
            client['alert_manager'] = manager

    def set_deferred_rewards_manager(self, manager):
        """Set the deferred rewards manager for P2P mode."""
        client = self._get_p2p_client()
        if client:
            client['deferred_rewards_manager'] = manager

    # New protocol feature manager setters

    def set_version_tracker(self, tracker):
        """Set the peer version tracker for protocol compatibility."""
        client = self._get_p2p_client()
        if client:
            client['version_tracker'] = tracker

    def set_version_negotiator(self, negotiator):
        """Set the version negotiator for protocol negotiation."""
        client = self._get_p2p_client()
        if client:
            client['version_negotiator'] = negotiator

    def set_storage_manager(self, manager):
        """Set the storage manager for redundant storage."""
        client = self._get_p2p_client()
        if client:
            client['storage_manager'] = manager

    def set_bandwidth_tracker(self, tracker):
        """Set the bandwidth tracker for monitoring."""
        client = self._get_p2p_client()
        if client:
            client['bandwidth_tracker'] = tracker

    def set_qos_manager(self, manager):
        """Set the QoS manager for bandwidth control."""
        client = self._get_p2p_client()
        if client:
            client['qos_manager'] = manager

    def get_protocol_version(self) -> str:
        """Get the current protocol version."""
        client = self._get_p2p_client()
        if client:
            return client.get('protocol_version', '1.0.0')
        return '1.0.0'

    def get_bandwidth_stats(self) -> dict:
        """Get current bandwidth statistics from QoS manager."""
        client = self._get_p2p_client()
        if client and client.get('bandwidth_tracker'):
            tracker = client['bandwidth_tracker']
            return {
                'global': tracker.get_global_metrics().to_dict() if hasattr(tracker, 'get_global_metrics') else {},
                'topics': {t: m.to_dict() for t, m in tracker.get_topic_metrics().items()} if hasattr(tracker, 'get_topic_metrics') else {},
            }
        return {'global': {}, 'topics': {}}

    def get_storage_status(self) -> dict:
        """Get current storage redundancy status."""
        client = self._get_p2p_client()
        if client and client.get('storage_manager'):
            manager = client['storage_manager']
            if hasattr(manager, 'get_status'):
                return manager.get_status()
        return {'status': 'unavailable'}