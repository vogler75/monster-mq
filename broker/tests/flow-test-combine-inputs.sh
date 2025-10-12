#!/bin/bash

# MonsterMQ Flow Engine Test Script
# Creates a flow class that combines two inputs into JSON and publishes to one output

GRAPHQL_URL="http://localhost:4000/graphql"

echo "=== MonsterMQ Flow Engine Test: Combine Inputs ==="
echo ""

# Check if server is running
echo "Checking if GraphQL server is running..."
if ! curl -s -m 5 http://localhost:4000/health > /dev/null 2>&1; then
  echo "✗ Error: GraphQL server is not responding at $GRAPHQL_URL"
  echo "  Please start the MonsterMQ broker first"
  exit 1
fi
echo "✓ Server is running"
echo ""

# Step 1: Create the Flow Class
echo "Creating flow class 'combine-test-type'..."

# JavaScript code for the combiner node
SCRIPT='// Wait for both inputs to have values
if (inputs.input1 && inputs.input2) {
  // Combine both inputs into a JSON object
  const combined = {
    input1: inputs.input1.value,
    input2: inputs.input2.value,
    timestamp: Date.now(),
    combined_at: new Date().toISOString()
  };

  console.log("Combining inputs:", JSON.stringify(combined));

  // Send to output
  outputs.send("output", combined);
} else {
  console.log("Waiting for both inputs...");
  console.log("input1:", inputs.input1 ? "present" : "missing");
  console.log("input2:", inputs.input2 ? "present" : "missing");
}'

# Build the GraphQL request as JSON
REQUEST=$(jq -n \
  --arg script "$SCRIPT" \
  '{
    query: "mutation CreateCombineFlow($script: String!) { flow { createClass(input: { name: \"combine-test-type\", namespace: \"test\", version: \"1.0.0\", description: \"Combines two inputs into JSON and publishes to output\", nodes: [{ id: \"combiner\", type: \"function\", name: \"Input Combiner\", language: \"javascript\", inputs: [\"input1\", \"input2\"], outputs: [\"output\"], config: { script: $script }, position: { x: 100, y: 100 } }], connections: [] }) { name namespace version description nodes { id name type language inputs outputs } createdAt } } }",
    variables: {
      script: $script
    }
  }')

RESULT=$(curl -s -X POST $GRAPHQL_URL \
  -H "Content-Type: application/json" \
  -d "$REQUEST")

echo "$RESULT" | jq '.'

if echo "$RESULT" | jq -e '.data.flow.createClass' > /dev/null 2>&1; then
  echo ""
  echo "✓ Flow class created successfully!"
  FLOW_CLASS_NAME=$(echo "$RESULT" | jq -r '.data.flow.createClass.name')
else
  echo ""
  echo "✗ Failed to create flow class"
  if echo "$RESULT" | jq -e '.errors' > /dev/null 2>&1; then
    echo "$RESULT" | jq '.errors'
  else
    echo "No error details available"
  fi
  exit 1
fi

echo ""

# Step 2: Create a Flow Instance
echo "Creating flow instance 'combine-test-instance'..."

REQUEST=$(jq -n '{
  query: "mutation CreateCombineInstance { flow { createInstance(input: { name: \"combine-test-instance\", namespace: \"test\", nodeId: \"local\", flowClassId: \"combine-test-type\", enabled: true, inputMappings: [{ nodeInput: \"combiner.input1\", type: TOPIC, value: \"test/input1\" }, { nodeInput: \"combiner.input2\", type: TOPIC, value: \"test/input2\" }], outputMappings: [{ nodeOutput: \"combiner.output\", topic: \"test/combined/output\" }], variables: { description: \"Test instance for combining inputs\" } }) { name namespace flowClassId enabled inputMappings { nodeInput type value } outputMappings { nodeOutput topic } createdAt } } }"
}')

RESULT=$(curl -s -X POST $GRAPHQL_URL \
  -H "Content-Type: application/json" \
  -d "$REQUEST")

echo "$RESULT" | jq '.'

if echo "$RESULT" | jq -e '.data.flow.createInstance' > /dev/null 2>&1; then
  echo ""
  echo "✓ Flow instance created successfully!"
else
  echo ""
  echo "✗ Failed to create flow instance"
  if echo "$RESULT" | jq -e '.errors' > /dev/null 2>&1; then
    echo "$RESULT" | jq '.errors'
  else
    echo "No error details available"
  fi
  exit 1
fi

echo ""
echo "=== Flow Setup Complete ==="
echo ""
echo "Test the flow by publishing messages:"
echo "  mosquitto_pub -t test/input1 -m '{\"temperature\": 23.5}'"
echo "  mosquitto_pub -t test/input2 -m '{\"humidity\": 65}'"
echo ""
echo "Subscribe to the output:"
echo "  mosquitto_sub -t test/combined/output"
echo ""
