/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"
#include <functional>
#include <algorithm>
#include <random>
#include <iterator>


/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5
#define NN 3

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes {
    JOINREQ,
    JOINREP,
    GOSSIP,
    DUMMYLASTMSGTYPE
};


inline const char *ToString(MsgTypes v) {
    switch (v) {
        case JOINREP:
            return "JOINREP";
        case JOINREQ:
            return "JOINREQ";
        case GOSSIP:
            return "GOSSIP";
        case DUMMYLASTMSGTYPE:
            return "DUMMYLASTMSGTYPE";
        default:
            return "[Unknown]";
    }
}

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
    enum MsgTypes msgType;
} MessageHeader;

//using messageHandler = std::function<void>(const Address *, const char *);
using messageHandler = std::function<void>();

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
    EmulNet *emulNet;
    Log *log;
    Params *par;
    Member *memberNode;
    vector<int> failedNodeIds;
    char NULLADDR[6];

    typedef void (MP1Node::*MessageHandler)(Address *, const char *);

    map<MsgTypes, MessageHandler> messageHandlersByType;

    void sendMessage(Address *to, MsgTypes type, char *payload, int payloadSize) const;

    void joinRequestHandler(Address *fromAddress, const char *data);

    void joinResponseHandler(Address *fromAddress, const char *data);

    void gossipMessageHandler(Address *fromAddress, const char *data);

public:
    class Serializer {
    private:
        const MemberListEntry &member;
    public:
        explicit Serializer(const MemberListEntry &memberListEntry) : member(memberListEntry) {}

        static size_t requiredBufferSize(const MemberListEntry &memberListEntry);

        void serialize(void *buffer) const;

        template<typename NUMBER>
        void pushNumber(void *&buffer, NUMBER number) const;
    };

    class Deserializer {
    private:
        const void *buffer_;
    public:
        explicit Deserializer(const char *buffer) : buffer_(buffer) {}

        MemberListEntry deserialize();
        template<typename NUMBER>
        NUMBER popNumber(const void *&buffer) const;
    };
    MP1Node(Member *, Params *, EmulNet *, Log *, Address *);

    Member *getMemberNode() {
        return memberNode;
    }

    int recvLoop();

    static int enqueueWrapper(void *env, char *buff, int size);

    void nodeStart(char *servaddrstr, short serverport);

    int initThisNode(Address *joinaddr);

    int introduceSelfToGroup(Address *joinAddress);

    int finishUpThisNode();

    void nodeLoop();

    void checkMessages();

    bool recvCallBack(void *env, char *data, int size);

    void nodeLoopOps();

    int isNullAddress(Address *addr);

    Address getJoinAddress();

    void initMemberListTable(Member *memberNode);

    void printAddress(Address *addr);

    virtual ~MP1Node();

    void joinRequest(const MessageHeader *message) const;

    char *
    serializeMembershipList(vector<MemberListEntry> &memberList) const;

    void gossip();

    int calculateId(const char *addr) const;

    Address createAddress(int id, const short i);

    template<typename Iter>
    Iter select_randomly(Iter start, Iter end);

    template<typename Iter, typename RandomGenerator>
    Iter select_randomly(Iter start, Iter end, RandomGenerator &g);

    bool joinNode(const MemberListEntry &joinedMember);

    MemberListEntry * joinNode(int id, short port, long heartbeat, long timestamp);

    vector<MemberListEntry>::iterator findById(int id);

    vector<int>::iterator findFailedById(int id);

    bool hasFailed(const MemberListEntry &member);

    bool shouldRemoveFailedNode(const vector<MemberListEntry>::iterator &failedNode);

    void removeNode(const vector<MemberListEntry>::iterator &failedNode);

    size_t calculatePayloadLength(const vector<MemberListEntry> &memberList) const;

    vector<MemberListEntry> memberListWithoutMeAndFailed();

    vector<MemberListEntry> selectRandomNodes(const vector<MemberListEntry> &nodesList, int numberOfNodesToSelect);

    vector<MemberListEntry> memberListWithoutFailed();

    bool hackForGrade{false};
};

#endif /* _MP1NODE_H_ */
