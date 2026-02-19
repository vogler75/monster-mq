# UI Update Plan: MQTT v5.0 Features

**Created:** January 29, 2026  
**Status:** Planning Phase  
**Related Issue:** #4 - Implement MQTT5 Support  
**Current Progress:** Backend 85% complete, UI 0%

---

## Overview

This document outlines the plan to update MonsterMQ's web dashboard UI to expose the newly implemented MQTT v5.0 features. The backend implementation is 85% complete with the following features ready for UI integration:

**Implemented Backend Features:**
1. ✅ Connection Properties (Phase 1-2)
2. ✅ Message Properties (Phase 3)
3. ✅ Topic Aliases (Phase 4)
4. ✅ Message Expiry (Phase 5)
5. ✅ Server Properties (Phase 7)
6. ✅ Flow Control - Receive Maximum (Phase 8)
7. ✅ No Local subscription option (Phase 8)
8. ✅ Retain Handling subscription option (Phase 8)
9. ✅ Payload Format Indicator validation (Phase 8)

---

## UI Components Requiring Updates

### 1. MQTT Client Detail Page
**File:** `broker/src/main/resources/dashboard/pages/mqtt-client-detail.html`

**Current State:**
- Shows basic connection info (host, port, username, enabled status)
- Displays topic mappings with mode (PUBLISH/SUBSCRIBE), QoS, remote/local topics
- No MQTT v5 specific options visible

**Required Updates:**

#### A. Connection Configuration Section
Add protocol version selector and MQTT v5 connection properties:

```html
<!-- New Section: MQTT Protocol Settings -->
<div class="form-group">
    <label for="mqtt-protocol-version">Protocol Version</label>
    <select id="mqtt-protocol-version" onchange="toggleMqtt5Options()">
        <option value="4">MQTT v3.1.1</option>
        <option value="5">MQTT v5.0</option>
    </select>
</div>

<!-- MQTT v5 Connection Properties (show only when v5 selected) -->
<div id="mqtt5-connect-properties" style="display: none;">
    <h3>MQTT v5.0 Connection Properties</h3>
    
    <div class="form-group">
        <label for="session-expiry">Session Expiry Interval (seconds)</label>
        <input type="number" id="session-expiry" min="0" max="4294967295" value="0">
        <small>0 = session ends on disconnect, >0 = persistent session</small>
    </div>
    
    <div class="form-group">
        <label for="receive-maximum">Receive Maximum</label>
        <input type="number" id="receive-maximum" min="1" max="65535" value="65535">
        <small>Maximum QoS 1/2 messages the client can process concurrently</small>
    </div>
    
    <div class="form-group">
        <label for="max-packet-size">Maximum Packet Size (bytes)</label>
        <input type="number" id="max-packet-size" min="1" max="268435455" value="268435455">
        <small>Maximum packet size the client accepts</small>
    </div>
    
    <div class="form-group">
        <label for="topic-alias-max">Topic Alias Maximum</label>
        <input type="number" id="topic-alias-max" min="0" max="65535" value="10">
        <small>Maximum number of topic aliases the client accepts (0 = disabled)</small>
    </div>
</div>
```

#### B. Topic Mapping Configuration Section
Enhance subscription options with MQTT v5 flags:

```html
<!-- Enhanced Subscription Options -->
<div id="mqtt5-subscription-options" style="display: none;">
    <h4>MQTT v5.0 Subscription Options</h4>
    
    <div class="checkbox-group">
        <input type="checkbox" id="sub-no-local">
        <label for="sub-no-local">
            No Local
            <span class="info-icon" title="Don't send messages back to the publisher">ⓘ</span>
        </label>
    </div>
    <small>Prevent receiving messages you published to this topic</small>
    
    <div class="form-group" style="margin-top: 1rem;">
        <label for="retain-handling">Retain Handling</label>
        <select id="retain-handling">
            <option value="0">Send retained messages (default)</option>
            <option value="1">Send retained only if new subscription</option>
            <option value="2">Never send retained messages</option>
        </select>
        <small>Controls when retained messages are delivered at subscribe time</small>
    </div>
    
    <div class="checkbox-group" style="margin-top: 1rem;">
        <input type="checkbox" id="retain-as-published">
        <label for="retain-as-published">
            Retain As Published (RAP)
            <span class="info-icon" title="Keep original retain flag">ⓘ</span>
        </label>
    </div>
    <small>Preserve the retain flag from the original message</small>
</div>
```

#### C. Message Properties Section (for publish mappings)
Add MQTT v5 message properties:

```html
<!-- Message Properties for PUBLISH mode -->
<div id="mqtt5-message-properties" style="display: none;">
    <h4>MQTT v5.0 Message Properties</h4>
    
    <div class="form-group">
        <label for="message-expiry">Message Expiry Interval (seconds)</label>
        <input type="number" id="message-expiry" min="0" value="0">
        <small>0 = no expiry, >0 = message expires after N seconds</small>
    </div>
    
    <div class="form-group">
        <label for="content-type">Content Type</label>
        <input type="text" id="content-type" placeholder="e.g., application/json">
        <small>MIME type of the payload (optional)</small>
    </div>
    
    <div class="form-group">
        <label for="response-topic">Response Topic</label>
        <input type="text" id="response-topic" placeholder="e.g., response/client123">
        <small>Topic for request-response pattern (optional)</small>
    </div>
    
    <div class="checkbox-group">
        <input type="checkbox" id="payload-format-utf8">
        <label for="payload-format-utf8">Payload is UTF-8 text</label>
    </div>
    <small>Enable UTF-8 validation for the payload</small>
    
    <div class="form-group" style="margin-top: 1rem;">
        <label>User Properties (key=value pairs)</label>
        <div id="user-properties-list"></div>
        <button type="button" class="btn btn-secondary btn-sm" onclick="addUserProperty()">
            + Add User Property
        </button>
    </div>
</div>
```

---

### 2. MQTT Clients List Page
**File:** `broker/src/main/resources/dashboard/pages/mqtt-clients.html`

**Required Updates:**

#### Add Protocol Version Column
Display which MQTT protocol version each client is using:

```html
<!-- Add to table header -->
<th>Protocol</th>

<!-- Add to table body -->
<td>
    <span class="protocol-badge protocol-v5">MQTT v5.0</span>
    <!-- or -->
    <span class="protocol-badge protocol-v3">MQTT v3.1.1</span>
</td>
```

#### Add CSS for protocol badges:
```css
.protocol-badge {
    padding: 0.3rem 0.6rem;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 600;
}
.protocol-v5 {
    background: rgba(124, 58, 237, 0.15);
    color: var(--monster-purple);
    border: 1px solid rgba(124, 58, 237, 0.3);
}
.protocol-v3 {
    background: rgba(59, 130, 246, 0.15);
    color: var(--monster-teal);
    border: 1px solid rgba(59, 130, 246, 0.3);
}
```

---

### 3. Dashboard Overview Page
**File:** `broker/src/main/resources/dashboard/pages/dashboard.html`

**Required Updates:**

#### Add MQTT v5 Statistics Card
Display MQTT v5 adoption and feature usage:

```html
<!-- New Metrics Card -->
<div class="metric-card">
    <span class="metric-title">MQTT v5.0 Clients</span>
    <span class="metric-value" id="mqtt5-clients">0</span>
    <span class="metric-subtitle" id="mqtt5-percentage">0% of total</span>
</div>

<div class="metric-card">
    <span class="metric-title">Active Topic Aliases</span>
    <span class="metric-value" id="topic-aliases-count">0</span>
    <span class="metric-subtitle">Bandwidth optimization active</span>
</div>

<div class="metric-card">
    <span class="metric-title">Messages with Expiry</span>
    <span class="metric-value" id="expiring-messages">0</span>
    <span class="metric-subtitle">TTL-managed messages</span>
</div>
```

---

## GraphQL API Updates

### Required GraphQL Queries/Mutations

#### 1. Query MQTT Client with v5 Properties
```graphql
query GetMqttClient($name: String!) {
  mqttClient(name: $name) {
    name
    enabled
    host
    port
    username
    protocolVersion    # NEW: 4 or 5
    
    # MQTT v5 Connection Properties
    sessionExpiryInterval
    receiveMaximum
    maxPacketSize
    topicAliasMaximum
    
    addresses {
      mode
      remoteTopic
      localTopic
      qos
      
      # MQTT v5 Subscription Options
      noLocal
      retainHandling
      retainAsPublished
      
      # MQTT v5 Message Properties (for PUBLISH mode)
      messageExpiryInterval
      contentType
      responseTopicPattern
      payloadFormatIndicator
      userProperties {
        key
        value
      }
    }
  }
}
```

#### 2. Mutation to Update/Create MQTT Client
```graphql
mutation SaveMqttClient($input: MqttClientInput!) {
  saveMqttClient(input: $input) {
    name
    enabled
    protocolVersion
  }
}

input MqttClientInput {
  name: String!
  enabled: Boolean
  host: String
  port: Int
  username: String
  password: String
  protocolVersion: Int    # 4 or 5
  
  # MQTT v5 Connection Properties
  sessionExpiryInterval: Int
  receiveMaximum: Int
  maxPacketSize: Int
  topicAliasMaximum: Int
  
  addresses: [AddressMappingInput!]
}

input AddressMappingInput {
  mode: String!
  remoteTopic: String!
  localTopic: String!
  qos: Int!
  
  # MQTT v5 Subscription Options
  noLocal: Boolean
  retainHandling: Int
  retainAsPublished: Boolean
  
  # MQTT v5 Message Properties
  messageExpiryInterval: Int
  contentType: String
  responseTopicPattern: String
  payloadFormatIndicator: Int
  userProperties: [UserPropertyInput!]
}

input UserPropertyInput {
  key: String!
  value: String!
}
```

#### 3. Query for Dashboard Statistics
```graphql
query GetMqtt5Statistics {
  mqtt5Statistics {
    totalClients
    mqtt5Clients
    mqtt5Percentage
    activeTopicAliases
    messagesWithExpiry
    noLocalSubscriptions
    retainHandlingSubscriptions
  }
}
```

---

## JavaScript Implementation Plan

### 1. Update GraphQL Client Queries
**File:** `broker/src/main/resources/dashboard/js/graphql-client.js` (or inline in pages)

Add new GraphQL queries for MQTT v5 properties:

```javascript
// Query with MQTT v5 fields
const MQTT_CLIENT_QUERY_V5 = `
  query GetMqttClient($name: String!) {
    mqttClient(name: $name) {
      name
      enabled
      host
      port
      username
      protocolVersion
      sessionExpiryInterval
      receiveMaximum
      maxPacketSize
      topicAliasMaximum
      addresses {
        mode
        remoteTopic
        localTopic
        qos
        noLocal
        retainHandling
        retainAsPublished
        messageExpiryInterval
        contentType
        responseTopicPattern
        payloadFormatIndicator
        userProperties { key value }
      }
    }
  }
`;
```

### 2. Form Handling Functions

**File:** `broker/src/main/resources/dashboard/pages/mqtt-client-detail.html` (inline scripts)

```javascript
function toggleMqtt5Options() {
    const version = document.getElementById('mqtt-protocol-version').value;
    const mqtt5Sections = document.querySelectorAll('[id^="mqtt5-"]');
    
    mqtt5Sections.forEach(section => {
        section.style.display = version === '5' ? 'block' : 'none';
    });
}

function addUserProperty() {
    const container = document.getElementById('user-properties-list');
    const propertyDiv = document.createElement('div');
    propertyDiv.className = 'user-property-row';
    propertyDiv.innerHTML = `
        <input type="text" placeholder="Key" class="user-prop-key">
        <input type="text" placeholder="Value" class="user-prop-value">
        <button type="button" onclick="this.parentElement.remove()">Remove</button>
    `;
    container.appendChild(propertyDiv);
}

function collectMqtt5Properties() {
    const version = document.getElementById('mqtt-protocol-version').value;
    
    if (version !== '5') return null;
    
    return {
        sessionExpiryInterval: parseInt(document.getElementById('session-expiry').value) || 0,
        receiveMaximum: parseInt(document.getElementById('receive-maximum').value) || 65535,
        maxPacketSize: parseInt(document.getElementById('max-packet-size').value) || 268435455,
        topicAliasMaximum: parseInt(document.getElementById('topic-alias-max').value) || 10
    };
}

function collectSubscriptionOptions() {
    const version = document.getElementById('mqtt-protocol-version').value;
    
    if (version !== '5') return null;
    
    return {
        noLocal: document.getElementById('sub-no-local').checked,
        retainHandling: parseInt(document.getElementById('retain-handling').value) || 0,
        retainAsPublished: document.getElementById('retain-as-published').checked
    };
}

function collectMessageProperties() {
    const version = document.getElementById('mqtt-protocol-version').value;
    
    if (version !== '5') return null;
    
    const userProperties = [];
    document.querySelectorAll('.user-property-row').forEach(row => {
        const key = row.querySelector('.user-prop-key').value;
        const value = row.querySelector('.user-prop-value').value;
        if (key && value) {
            userProperties.push({ key, value });
        }
    });
    
    return {
        messageExpiryInterval: parseInt(document.getElementById('message-expiry').value) || 0,
        contentType: document.getElementById('content-type').value || null,
        responseTopicPattern: document.getElementById('response-topic').value || null,
        payloadFormatIndicator: document.getElementById('payload-format-utf8').checked ? 1 : 0,
        userProperties: userProperties.length > 0 ? userProperties : null
    };
}
```

---

## Backend GraphQL Schema Updates

### File: `broker/src/main/kotlin/graphql/MqttClientResolver.kt`

**Required Changes:**

1. Add MQTT v5 fields to `MqttClient` type
2. Update input types for mutations
3. Add resolver logic for new fields

```kotlin
// Example field additions to MqttClient type
@GraphQLDescription("MQTT protocol version (4 = v3.1.1, 5 = v5.0)")
fun protocolVersion(): Int = config.getInteger("ProtocolVersion", 4)

@GraphQLDescription("MQTT v5 Session Expiry Interval in seconds")
fun sessionExpiryInterval(): Int? = 
    if (protocolVersion() == 5) config.getInteger("SessionExpiryInterval") else null

@GraphQLDescription("MQTT v5 Receive Maximum for flow control")
fun receiveMaximum(): Int? = 
    if (protocolVersion() == 5) config.getInteger("ReceiveMaximum") else null

// ... additional v5 properties
```

---

## Testing Plan

### 1. Unit Tests for UI Components
- [ ] Test protocol version toggle shows/hides MQTT v5 sections
- [ ] Test form validation for MQTT v5 numeric fields (ranges)
- [ ] Test user property add/remove functionality
- [ ] Test data serialization to GraphQL mutation format

### 2. Integration Tests
- [ ] Create MQTT v3.1.1 client through UI
- [ ] Create MQTT v5 client with all properties through UI
- [ ] Update existing client from v3.1.1 to v5
- [ ] Verify MQTT v5 properties are saved and loaded correctly
- [ ] Test subscription options (noLocal, retainHandling)
- [ ] Test message properties (expiry, contentType, userProperties)

### 3. Visual/UX Testing
- [ ] Verify responsive design on mobile/tablet
- [ ] Check dark theme consistency for new elements
- [ ] Validate tooltip and help text clarity
- [ ] Test form accessibility (keyboard navigation)

---

## Implementation Phases

### Phase 1: Basic Protocol Version Support (Week 1)
- [ ] Add protocol version selector to client detail page
- [ ] Add MQTT v5 connection properties form section
- [ ] Update GraphQL schema with basic v5 fields
- [ ] Implement show/hide toggle logic
- [ ] Add protocol version badge to clients list

**Estimated Effort:** 2-3 days

### Phase 2: Subscription Options (Week 1-2)
- [ ] Add No Local checkbox
- [ ] Add Retain Handling dropdown
- [ ] Add Retain As Published checkbox
- [ ] Update GraphQL mutation to save subscription options
- [ ] Test subscription option functionality

**Estimated Effort:** 2-3 days

### Phase 3: Message Properties (Week 2)
- [ ] Add message expiry input
- [ ] Add content type input
- [ ] Add response topic input
- [ ] Add payload format indicator checkbox
- [ ] Implement user properties add/remove UI
- [ ] Update GraphQL mutation for message properties

**Estimated Effort:** 2-3 days

### Phase 4: Dashboard Statistics (Week 2-3)
- [ ] Add MQTT v5 metrics cards to dashboard
- [ ] Implement GraphQL query for v5 statistics
- [ ] Add real-time updates via GraphQL subscriptions
- [ ] Create visual charts for v5 adoption

**Estimated Effort:** 2 days

### Phase 5: Documentation & Polish (Week 3)
- [ ] Add help icons and tooltips for all v5 features
- [ ] Create user documentation
- [ ] Add inline examples and validation hints
- [ ] Final UX polish and testing

**Estimated Effort:** 1-2 days

**Total Estimated Effort:** 2-3 weeks

---

## UI/UX Design Guidelines

### Color Scheme for MQTT v5 Elements
- **MQTT v5 Badge:** Purple gradient (matches existing purple theme)
- **Info Icons:** Monster teal (#14B8A6)
- **Validation Success:** Monster green (#10B57D)
- **Validation Error:** Monster red (#EF4444)

### Form Layout Principles
1. **Progressive Disclosure:** Show v5 options only when v5 selected
2. **Contextual Help:** Tooltips and inline hints for all v5 features
3. **Smart Defaults:** Pre-fill recommended values (e.g., receiveMaximum=65535)
4. **Visual Hierarchy:** Group related properties in collapsible sections

### Accessibility Requirements
- All form fields must have proper labels
- Color coding must be supplemented with text/icons
- Keyboard navigation must work for all interactive elements
- Screen reader compatible ARIA labels

---

## Documentation Updates Required

### 1. User Guide
- [ ] Add "MQTT v5.0 Features" chapter
- [ ] Document connection properties and their effects
- [ ] Explain subscription options with use cases
- [ ] Provide message properties examples
- [ ] Add request-response pattern guide

### 2. API Documentation
- [ ] Update GraphQL schema documentation
- [ ] Add MQTT v5 examples to API guide
- [ ] Document property ranges and validation rules

### 3. Migration Guide
- [ ] Document upgrading from v3.1.1 to v5
- [ ] List breaking changes (if any)
- [ ] Provide migration checklist

---

## Risks & Mitigation

### Risk 1: Backend Schema Changes Required
**Mitigation:** Review current GraphQL schema and identify gaps before starting UI work

### Risk 2: Backward Compatibility with Existing Clients
**Mitigation:** 
- Keep protocol version optional, default to v3.1.1
- Ensure v5 fields are nullable and ignored for v3.1.1 clients

### Risk 3: Complex Form Validation
**Mitigation:**
- Implement client-side validation with clear error messages
- Add server-side validation as safety net

### Risk 4: User Confusion About v5 Features
**Mitigation:**
- Extensive tooltips and inline help
- Link to MQTT v5 spec documentation
- Provide common use case examples

---

## Success Metrics

### Feature Adoption
- Track % of clients configured with MQTT v5
- Monitor usage of specific v5 features (noLocal, retainHandling, etc.)

### User Experience
- Time to configure first MQTT v5 client (target: <5 minutes)
- Support ticket reduction related to MQTT configuration
- User feedback/satisfaction surveys

### Performance
- UI load time impact (target: <50ms additional)
- Form submission latency (target: <200ms)

---

## References

- [MQTT v5.0 Specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [MonsterMQ MQTT5_IMPLEMENTATION_PLAN.md](MQTT5_IMPLEMENTATION_PLAN.md)
- Current UI Dashboard: `broker/src/main/resources/dashboard/`

---

## Next Steps

1. **Review & Approval:** Get stakeholder approval on UI design approach
2. **GraphQL Schema Update:** Implement backend schema changes first
3. **UI Prototyping:** Create mockups for key screens
4. **Phased Implementation:** Start with Phase 1 (basic protocol support)
5. **Continuous Testing:** Test after each phase completion

---

*Last Updated: January 29, 2026*
