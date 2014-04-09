Implemented distributed directory service as the lookup service

lookup service acts as broker for service addresses

Services register with the broker

Broker then provides registered addresses to clients

Clients then communicate directly to services using provided addresses