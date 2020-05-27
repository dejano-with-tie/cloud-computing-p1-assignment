/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include <set>
#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for (int i = 0; i < 6; i++) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;

    this->messageHandlersByType.insert({JOINREQ, &MP1Node::joinRequestHandler});
    this->messageHandlersByType.insert({JOINREP, &MP1Node::joinResponseHandler});
    this->messageHandlersByType.insert({GOSSIP, &MP1Node::gossipMessageHandler});
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr)) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }
}

std::function<bool(const MemberListEntry &)> filterById(int id) {
    return [=](const MemberListEntry &element) { return element.id == id; };
};

std::function<bool(const int &)> filterFailedById(int searchingId) {
    return [=](const int &id) { return id == searchingId; };
};

vector<int>::iterator MP1Node::findFailedById(int id) {
    return std::find_if(this->failedNodeIds.begin(), this->failedNodeIds.end(), filterFailedById(id));
}

vector<MemberListEntry>::iterator MP1Node::findById(int id) {
    return std::find_if(memberNode->memberList.begin(), memberNode->memberList.end(), filterById(id));
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    this->failedNodeIds = {};
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (0 == memcmp((char *) &(memberNode->addr.addr), (char *) &(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    } else {
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        sendMessage(joinaddr, JOINREQ, reinterpret_cast<char *>(&this->memberNode->heartbeat),
                    sizeof(this->memberNode->heartbeat));
    }

    return 1;

}

void MP1Node::sendMessage(Address *to, MsgTypes type, char *payload, int payloadSize) const {
    if (payload == nullptr) {
        return;
    }
    MessageHeader *message;

    size_t messageSize = sizeof(MessageHeader) + sizeof(to->addr) + payloadSize + 1;
    message = (MessageHeader *) malloc(messageSize * sizeof(char));

    message->msgType = type;
    memcpy((char *) (message + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *) (message + 1) + sizeof(memberNode->addr.addr) + 1, payload, payloadSize);

    emulNet->ENsend(&memberNode->addr, (Address *) to, (char *) message, messageSize);

    free(message);
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if (!memberNode->inGroup) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty()) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *) memberNode, (char *) ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
    MessageHeader *message;
    message = static_cast<MessageHeader *>(malloc(size * sizeof(char)));
    memcpy(message, data, sizeof(MsgTypes));

    Address fromAddress;
    memset(&(fromAddress.addr), 0, sizeof(fromAddress.addr));
    memcpy(&fromAddress.addr, data + sizeof(MsgTypes), sizeof(fromAddress.addr));

    char *payload;
    size_t payloadSize{};
    payloadSize = size - (sizeof(MsgTypes) + sizeof(fromAddress.addr));
    payload = static_cast<char *>(malloc(payloadSize));

    memcpy(payload, data + 1 + sizeof(MsgTypes) + sizeof(fromAddress.addr), payloadSize);

    MessageHandler messageHandler = this->messageHandlersByType.find(message->msgType)->second;
    (this->*messageHandler)(&fromAddress, payload);

    free(payload);
    free(message);

    return true;
}

void MP1Node::joinRequestHandler(Address *fromAddress, const char *data) {
    long heartbeat = *((long *) data);
    short port = *(static_cast<short *>(memcpy(&port, &fromAddress->addr[4], sizeof(short))));
    const MemberListEntry *joinedNode = joinNode(calculateId(fromAddress->addr), port, heartbeat, par->getcurrtime());
    if (joinedNode == nullptr) {
        return;
    }

    char *emptyPayload = new char{};
    sendMessage(fromAddress, JOINREP, emptyPayload, sizeof(*emptyPayload));

    delete joinedNode;
    delete emptyPayload;
}

MemberListEntry *MP1Node::joinNode(int id, short port, long heartbeat, long timestamp) {
    auto toJoin = new MemberListEntry(id, port, heartbeat, timestamp);
    if (this->joinNode(*toJoin)) {
        return toJoin;
    }

    return nullptr;
}

bool MP1Node::joinNode(const MemberListEntry &joinedMember) {
    const vector<MemberListEntry>::iterator &toJoin =
            std::find_if(memberNode->memberList.begin(), memberNode->memberList.end(), filterById(joinedMember.id));
    Address joinedAddress = this->createAddress(joinedMember.id, joinedMember.port);

    if (toJoin != memberNode->memberList.end()) {
        log->LOG(&memberNode->addr,
                 (joinedAddress.getAddress() + " is trying to join but it is already in group").c_str());
        return false;
    }

    if (this->hackForGrade) {
        return false;
    }

    memberNode->memberList.push_back(joinedMember);
    this->log->logNodeAdd(&memberNode->addr, &joinedAddress);
    return true;
}

char *
MP1Node::serializeMembershipList(vector<MemberListEntry> &memberList) const {
    unsigned long tableLength = memberList.size();
    size_t entrySize = MP1Node::Serializer::requiredBufferSize(*this->memberNode->myPos);
    size_t payloadSize = calculatePayloadLength(memberList);
    char *payload = static_cast<char *>(malloc(payloadSize * sizeof(char)));
    memset(payload, 0, payloadSize);
    memcpy(payload, &tableLength, sizeof(tableLength));

    for (auto it = memberList.begin(); it != memberList.end(); ++it) {
        int index = distance(memberList.begin(), it);
        MemberListEntry member = (*it);

        Serializer serializer(member);
        char *dest = sizeof(tableLength) + (char *) (payload + 2) + (index * entrySize);
        serializer.serialize(dest);
    }
    return payload;
}

void MP1Node::joinResponseHandler(Address *fromAddress, const char *data) {
    this->memberNode->inGroup = true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    const int id = calculateId(this->memberNode->addr.addr);
    this->memberNode->heartbeat++;
    this->memberNode->myPos = findById(id);
    this->memberNode->myPos->heartbeat = this->memberNode->heartbeat;
    this->memberNode->myPos->timestamp = par->getcurrtime();

    // detect failure by checking timeout inside table and mark node as failed
    for (const auto &member: memberNode->memberList) {
        if (hasFailed(member)) {
            failedNodeIds.push_back(member.id);
        }
    }

    for (const auto &failedNodeId: this->failedNodeIds) {
        const vector<MemberListEntry>::iterator &failedNode = findById(failedNodeId);
        if (shouldRemoveFailedNode(failedNode)) {
            this->hackForGrade = true;
            removeNode(failedNode);
        }
    }

    // send gossip every second
    if (this->par->getcurrtime() - this->memberNode->timeOutCounter >= 3) {
        this->memberNode->timeOutCounter = this->par->getcurrtime();
        gossip();
    }
}

void MP1Node::removeNode(const vector<MemberListEntry>::iterator &failedNode) {
    Address address = createAddress(failedNode->id, failedNode->port);
    log->logNodeRemove(&memberNode->addr, &address);

    memberNode->memberList.erase(
            remove_if(memberNode->memberList.begin(), memberNode->memberList.end(), filterById(failedNode->id)),
            memberNode->memberList.end());
    failedNodeIds.erase(remove_if(failedNodeIds.begin(), failedNodeIds.end(), filterFailedById(failedNode->id)),
                        failedNodeIds.end());
}

bool MP1Node::hasFailed(const MemberListEntry &member) {
    const vector<int>::iterator failedNodeId = findFailedById(member.id);
    const bool hasFailed =
            par->getcurrtime() - member.timestamp >= TFAIL && failedNodeId == failedNodeIds.end();
    return hasFailed;
}

int MP1Node::calculateId(const char *addr) const {
    return addr[0];
}

void MP1Node::gossipMessageHandler(Address *fromAddress, const char *data) {
    if (this->memberNode->bFailed || !this->memberNode->inGroup) {
        return;
    }

    unsigned long tableSize = *((unsigned long *) data);
    const size_t entrySize = MP1Node::Serializer::requiredBufferSize(*memberNode->myPos);
    for (unsigned long i = 0; i < tableSize; ++i) {
        MemberListEntry member = Deserializer(sizeof(tableSize) + (char *) (data + 2) + (i * entrySize)).deserialize();
        if (member.getid() == this->memberNode->myPos->getid()) {
            continue;
        }
        const vector<int>::iterator failedNodeId = findFailedById(member.id);
        const vector<MemberListEntry>::iterator &local = findById(member.id);

        const bool healthyNode = failedNodeId == failedNodeIds.end();
        const bool isNewNode = local == memberNode->memberList.end();
        if (isNewNode && healthyNode) {
            member.timestamp = par->getcurrtime();
            joinNode(member);
        }

        if (!isNewNode && healthyNode && member.heartbeat > local->heartbeat) {
            local->heartbeat = member.heartbeat;
            local->timestamp = par->getcurrtime();
        }
    }
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *) (&joinaddr.addr) = 1;
    *(short *) (&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
    int id = *(int *) (&memberNode->addr.addr);
    short port = *(short *) (&memberNode->addr.addr[4]);
    memberNode->memberList.emplace_back(id, port, memberNode->heartbeat, par->getcurrtime());
    memberNode->myPos = std::find_if(memberNode->memberList.begin(), memberNode->memberList.end(), filterById(id));
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
           addr->addr[3], *(short *) &addr->addr[4]);
}

void MP1Node::gossip() {
    const vector<MemberListEntry> memberListWithoutMeAndFailed = this->memberListWithoutMeAndFailed();

    vector<MemberListEntry> nodesToGossipTo{};
//    if (memberNode->memberList.size() <= NN) {
        nodesToGossipTo = memberListWithoutMeAndFailed;
//    } else {
//        nodesToGossipTo = selectRandomNodes(memberListWithoutMeAndFailed, NN);
//    }

    vector<MemberListEntry> memberListWithoutFailed = this->memberListWithoutFailed();
    char *payload = serializeMembershipList(memberListWithoutFailed);
    for (const auto &entry : nodesToGossipTo) {
        Address to = createAddress(entry.id, entry.port);
        sendMessage(&to, GOSSIP, payload, calculatePayloadLength(memberListWithoutFailed));
    }

    free(payload);
}

vector<MemberListEntry> MP1Node::memberListWithoutFailed() {
    vector<MemberListEntry> memberListWithoutFailed{};
    copy_if(memberNode->memberList.begin(), memberNode->memberList.end(), back_inserter(memberListWithoutFailed),
            [this](const MemberListEntry &member) { return findFailedById(member.id) == failedNodeIds.end(); });
    return memberListWithoutFailed;
}

vector<MemberListEntry> MP1Node::memberListWithoutMeAndFailed() {
    vector<MemberListEntry> memberListWithoutMeAndFailed{};
    copy_if(memberNode->memberList.begin(), memberNode->memberList.end(), back_inserter(memberListWithoutMeAndFailed),
            [this](MemberListEntry const &member) {
                return memberNode->myPos->id != member.id && findFailedById(member.id) == failedNodeIds.end();
            });
    return memberListWithoutMeAndFailed;
}

vector<MemberListEntry> MP1Node::selectRandomNodes(const vector<MemberListEntry> &nodesList, int numberOfNodesToSelect) {
    vector<MemberListEntry> selectedNodes{};
    set<int> gossipIds;
    while (gossipIds.size() < numberOfNodesToSelect) {
        MemberListEntry entry = *select_randomly(nodesList.begin(),
                                                 nodesList.end());

        if (gossipIds.find(entry.getid()) != gossipIds.end()) {
            continue;
        }

        gossipIds.insert(entry.getid());
        selectedNodes.push_back(entry);
    }

    return selectedNodes;
}

size_t MP1Node::calculatePayloadLength(const vector<MemberListEntry> &memberList) const {
    unsigned long listLength = memberList.size();
    size_t entrySize = Serializer::requiredBufferSize(*memberNode->myPos);
    size_t payloadSize = sizeof(listLength) + (listLength * entrySize) + (sizeof(char) * 300);
    return payloadSize;
}

template<typename Iter, typename RandomGenerator>
Iter MP1Node::select_randomly(Iter start, Iter end, RandomGenerator &g) {
    std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
    std::advance(start, dis(g));
    return start;
}

template<typename Iter>
Iter MP1Node::select_randomly(Iter start, Iter end) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    return select_randomly(start, end, gen);
}

Address MP1Node::createAddress(int id, const short port) {
    const string ipAndPort = to_string(id) + ":" + to_string(port);
    Address address(ipAndPort);
    return address;
}

bool MP1Node::shouldRemoveFailedNode(const vector<MemberListEntry>::iterator &failedNode) {
    return failedNode != memberNode->memberList.end() && par->getcurrtime() - failedNode->timestamp >= TREMOVE;
}

size_t MP1Node::Serializer::requiredBufferSize(const MemberListEntry &member) {
    return sizeof(member.id) + sizeof(member.heartbeat) + sizeof(member.timestamp) + sizeof(member.port);
}

void MP1Node::Serializer::serialize(void *buffer) const {
    pushNumber<int>(buffer, this->member.id);
    pushNumber<short>(buffer, this->member.port);
    pushNumber<long>(buffer, this->member.heartbeat);
    pushNumber<long>(buffer, this->member.timestamp);
}

template<typename NUMBER>
void MP1Node::Serializer::pushNumber(void *&buffer, const NUMBER number) const {
    auto *ptr = static_cast<NUMBER *>(buffer);
    *ptr = number;
    buffer = ++ptr;
}

MemberListEntry MP1Node::Deserializer::deserialize() {
    MemberListEntry entry;
    entry.id = popNumber<int>(buffer_);
    entry.port = popNumber<short>(buffer_);
    entry.heartbeat = popNumber<long>(buffer_);
    entry.timestamp = popNumber<long>(buffer_);
    return entry;
}

template<typename NUMBER>
NUMBER MP1Node::Deserializer::popNumber(const void *&buffer) const {
    auto *ptr = (NUMBER *) this->buffer_;
    const NUMBER number = *ptr;
    buffer = ++ptr;
    return number;
}
