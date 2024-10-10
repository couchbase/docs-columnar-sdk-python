from couchbase_columnar.cluster import Cluster
from couchbase_columnar.options import ClusterOptions, SecurityOptions
from couchbase_columnar.credential import Credential

from acouchbase_columnar.cluster import AsyncCluster


CONNECTION_STRING = 'couchbases://192.168.106.130'
USERNAME = 'Administrator'
PASSWORD = 'password'


def get_cluster():
    cred = Credential.from_username_and_password(USERNAME, PASSWORD)
    security_options = SecurityOptions(disable_server_certificate_verification=True)
    options = ClusterOptions(security_options=security_options)
    return Cluster.create_instance(CONNECTION_STRING, cred, options)


def get_asyncio_cluster():
    cred = Credential.from_username_and_password(USERNAME, PASSWORD)
    security_options = SecurityOptions(disable_server_certificate_verification=True)
    options = ClusterOptions(security_options=security_options)
    return AsyncCluster.create_instance(CONNECTION_STRING, cred, options)
