# OPC UA Test Recovery Script
# This script will properly configure the OPC UA server for testing

Write-Host "`n=== OPC UA Server Test Setup ===" -ForegroundColor Cyan

$graphqlUrl = "http://localhost:4000/graphql"

function Invoke-GraphQL($query) {
    try {
        $body = @{query=$query} | ConvertTo-Json
        $response = Invoke-RestMethod -Uri $graphqlUrl -Method Post -Body $body -ContentType 'application/json'
        return $response
    } catch {
        Write-Host "GraphQL Error: $_" -ForegroundColor Red
        return $null
    }
}

# Step 1: Delete existing server if any
Write-Host "`n[1/5] Deleting existing test-server..." -ForegroundColor Yellow
$deleteQuery = 'mutation { opcUaServer { delete(serverName: "test-server") { success message } } }'
$result = Invoke-GraphQL $deleteQuery
if ($result) { Write-Host "  $($result.data.opcUaServer.delete.message)" -ForegroundColor Green }

Write-Host "  Waiting 5 seconds for server to fully stop..." -ForegroundColor Gray
Start-Sleep -Seconds 5

# Step 2: Create new server
Write-Host "`n[2/5] Creating new OPC UA server..." -ForegroundColor Yellow
$createQuery = @'
mutation {
  opcUaServer {
    create(config: {
      name: "test-server"
      namespace: "opcua/server"
      nodeId: "*"
      enabled: true
      port: 4841
      path: "server"
      namespaceUri: "urn:MonsterMQ:OpcUaServer"
      updateInterval: 100
      bufferSize: 1000
    }) {
      success
      message
    }
  }
}
'@
$result = Invoke-GraphQL $createQuery
if ($result.data.opcUaServer.create.success) {
    Write-Host "  * Server created successfully" -ForegroundColor Green
} else {
    Write-Host "  * Failed to create server" -ForegroundColor Red
    exit 1
}

Start-Sleep -Seconds 3

# Step 3: Add write/# mapping
Write-Host "`n[3/5] Adding write/# address mapping..." -ForegroundColor Yellow
$writeQuery = @'
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "write/#"
      browseName: "write"
      dataType: TEXT
      accessLevel: READ_WRITE
      description: "Writable text values"
    }) {
      success
      message
    }
  }
}
'@
$result = Invoke-GraphQL $writeQuery
if ($result) { Write-Host "  * write/# mapping added" -ForegroundColor Green }

# Step 4: Add float/# mapping
Write-Host "`n[4/5] Adding float/# address mapping..." -ForegroundColor Yellow
$floatQuery = @'
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "float/#"
      browseName: "float"
      dataType: NUMERIC
      accessLevel: READ_WRITE
      description: "Numeric sensor values"
    }) {
      success
      message
    }
  }
}
'@
$result = Invoke-GraphQL $floatQuery
if ($result) { Write-Host "  * float/# mapping added" -ForegroundColor Green }

# Step 5: Verify configuration
Write-Host "`n[5/5] Verifying configuration..." -ForegroundColor Yellow
$verifyQuery = 'query { opcUaServers { name enabled port addresses { mqttTopic dataType } } }'
$result = Invoke-GraphQL $verifyQuery
if ($result -and $result.data.opcUaServers) {
    $server = $result.data.opcUaServers[0]
    Write-Host "  Server: $($server.name)" -ForegroundColor Cyan
    Write-Host "  Port: $($server.port)" -ForegroundColor Cyan
    Write-Host "  Enabled: $($server.enabled)" -ForegroundColor Cyan
    Write-Host "  Address Mappings:" -ForegroundColor Cyan
    foreach ($addr in $server.addresses) {
        Write-Host "    - $($addr.mqttTopic) ($($addr.dataType))" -ForegroundColor Cyan
    }
}

Write-Host "`n=== Setup Complete ===" -ForegroundColor Green
Write-Host "Endpoint: opc.tcp://localhost:4841/server" -ForegroundColor Yellow
Write-Host ""
