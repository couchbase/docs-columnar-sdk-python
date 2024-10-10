#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# tag::logging[]
import logging

import couchbase_columnar
from couchbase_columnar.cluster import Cluster
from couchbase_columnar.credential import Credential

# output log messages to example.log
logging.basicConfig(filename='example.log',
                    filemode='w', 
                    level=logging.DEBUG,
                    format='%(levelname)s::%(asctime)s::%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)
# Good to set the logger's level because if the root level is not set, only WARNING level and above logs are output.
logger.setLevel(logging.DEBUG)
couchbase_columnar.configure_logging(logger.name, level=logger.level) 


def main() -> None:
    # Update this to your cluster
    connstr = 'couchbases://--your-instance--'
    username = 'username'
    pw = 'Password!123'
    # User Input ends here.

    cred = Credential.from_username_and_password(username, pw)
    cluster = Cluster.create_instance(connstr, cred)

    # Execute a query and buffer all result rows in client memory.
    statement = 'SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;'
    res = cluster.execute_query(statement)
    all_rows = res.get_all_rows()
    for row in all_rows:
        logger.info(f'Found row: {row}')
    logger.info(f'metadata={res.metadata()}')

if __name__ == '__main__':
    main()
# end::logging[]
