#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <cassert>
#include <errno.h>
#include <vector>
#include<poll.h>
#include <fcntl.h>
#include <cstring>
#include <algorithm>
#include <signal.h>
#include <stdint.h>
#include <string>
#include <map>
#include "hashtable.h"


#define RES_OK 0
#define RES_ERR 1
#define RES_NX 2
#define container_of(ptr, T, member) \
    ((T *)( (char *)ptr - offsetof(T, member) ))

const size_t k_max_msg=32<<20; // maximum message size 32MB

const uint32_t k_max_args=10; 




static void die(const char *msg){ 
    printf("%s\n",msg);
    exit(1);
}


static struct {
    HMap db;    // top-level hashtable
} g_data;


struct Entry {
    struct HNode node;  // hashtable node
    std::string key;
    std::string val;
};

struct Response {
    uint32_t status=0;
    std::vector<uint8_t> data;
};

enum {
    TAG_NIL = 0,    // nil
    TAG_ERR = 1,    // error code + msg
    TAG_STR = 2,    // string
    TAG_INT = 3,    // int64
    TAG_DBL = 4,    // double
    TAG_ARR = 5,    // array
};


struct Buffer{
    uint8_t *buffer_begin=nullptr;
    uint8_t *buffer_end=nullptr;
    uint8_t *data_begin=nullptr;
    uint8_t *data_end=nullptr;
    Buffer()=default;
    Buffer(const Buffer &) = delete;
    Buffer &operator=(const Buffer &) = delete;
    ~Buffer(){free(buffer_begin);}
};


struct conn {
    int fd=-1;
    bool want_read=false;
    bool want_write=false;
    bool want_close=false;
    struct Buffer incoming_buffer;
    struct Buffer outgoing_buffer;
};

enum {
    ERR_UNKNOWN = 1,    // unknown command
    ERR_TOO_BIG = 2,    // response too big
};



static bool entry_eq(HNode *lhs, HNode *rhs) {
    struct Entry *le = container_of(lhs, struct Entry, node);
    struct Entry *re = container_of(rhs, struct Entry, node);
    return le->key == re->key;
}


static size_t buf_size(const Buffer &buf) {
    return buf.data_end - buf.data_begin;
}



static bool read_size(const uint8_t *&curr,const uint8_t *end,uint32_t &out){
    if((curr+4)>end){
        return false;
    }
    memcpy(&out,curr,4);
    curr+=4;
    return true;
}

static bool read_str(const uint8_t *&curr,const uint8_t *end,size_t n,std::string &out){
    if(curr+n>end){
        return false;
    }
    out.assign(curr,curr+n);
    curr+=n;
    return true;
}


static void buf_append(struct Buffer &buf, const uint8_t *data, size_t len) {
    size_t size = buf_size(buf);
    size_t need=size+len;
    size_t cap=(buf.buffer_end-buf.buffer_begin);
    size_t back_room=buf.buffer_end-buf.data_end;

    if(back_room<len){
        if(need<=cap){
            std::memmove(
            buf.buffer_begin,
            buf.data_begin,
            size
            );
        }
        else{
            size_t new_cap=cap?cap:256;
            while(new_cap<need){
                new_cap*=2;
            }
            uint8_t *p=(uint8_t *)malloc(new_cap);
            if(!p){
                die("malloc()");
            }
            if(size){
                memcpy(p,buf.data_begin,size);
            }
            free(buf.buffer_begin);
            buf.buffer_begin=p;
            buf.buffer_end=p+new_cap;
        }
        buf.data_begin=buf.buffer_begin;
        buf.data_end=buf.data_begin+size;
    }
    memcpy(buf.data_end,data,len);
    buf.data_end+=len;
    
}

static void buf_resize(struct Buffer &buf, size_t new_size) {
    size_t size = buf_size(buf);
    if (new_size < size) {
        buf.data_end = buf.data_begin + new_size;
        return;
    }
    size_t cap = buf.buffer_end - buf.buffer_begin;
    if (new_size > cap) {
        size_t new_cap = cap ? cap : 256;
        while (new_cap < new_size) {
            new_cap *= 2;
        }
        uint8_t *p = (uint8_t *)malloc(new_cap);
        if (!p) {
            die("malloc()");
        }
        if (size) {
            memcpy(p, buf.data_begin, size);
        }
        free(buf.buffer_begin);
        buf.buffer_begin = p;
        buf.buffer_end = p + new_cap;
    }
    buf.data_begin = buf.buffer_begin;
    buf.data_end = buf.data_begin + new_size;
}


static void buf_append_u8(Buffer &buf, uint8_t data) {
    buf_append(buf, (const uint8_t *)&data, 1);
}

static void buf_append_u32(Buffer &buf, uint32_t data) {
    buf_append(buf, (const uint8_t *)&data, 4); // assume littlen-endian
}

static void buf_append_i64(Buffer &buf, int64_t data) {
    buf_append(buf, (const uint8_t *)&data, 8); 
}

static void out_nil(Buffer &out) {
    buf_append_u8(out, TAG_NIL);
}

static void out_int(Buffer &out, int64_t val) {
    buf_append_u8(out, TAG_INT);
    buf_append_i64(out, val);
}

static void out_str(Buffer &out, const char *s, size_t size) {
    buf_append_u8(out, TAG_STR);
    buf_append_u32(out, (uint32_t)size);
    buf_append(out, (const uint8_t *)s, size);
}

static void out_arr(Buffer &out, uint32_t n) {
    buf_append_u8(out, TAG_ARR);
    buf_append_u32(out, n);
}

static void out_err(Buffer &out, uint32_t code, const std::string &msg) {
    buf_append_u8(out, TAG_ERR);
    buf_append_u32(out, code);
    buf_append_u32(out, (uint32_t)msg.size());
    buf_append(out, (const uint8_t *)msg.data(), msg.size());
}

static void response_begin(Buffer &out, size_t *header) {
    *header = buf_size(out);       // messege header position
    buf_append_u32(out, 0);     // reserve space
}

static size_t response_size(Buffer &out, size_t header) {
    return buf_size(out) - header - 4;
}

static void response_end(Buffer &out, size_t header) {
    size_t msg_size = response_size(out, header);
    if (msg_size > k_max_msg) {
        buf_resize(out, header + 4);
        out_err(out, ERR_TOO_BIG, "response is too big.");
        msg_size = response_size(out, header);
    }
    // message header
    uint32_t len = (uint32_t)msg_size;
    memcpy(&out.buffer_begin[header], &len, 4);
}

static bool cb_keys(HNode *node, void *arg) {
    Buffer &out = *(Buffer *)arg;
    const std::string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

static void do_keys(std::vector<std::string> &, Buffer &out) {
    out_arr(out, (uint32_t)hm_size(&g_data.db));
    hm_foreach(&g_data.db, &cb_keys, (void *)&out);
}

static int32_t parse_req(const uint8_t *data,size_t size,std::vector<std::string> &out){
    const uint8_t *end=data+size;
    uint32_t nstr=0;
    if(!read_size(data,end,nstr)){
        return -1;
    }
    if(nstr>k_max_args){
        return -1;
    }
    while(out.size()<nstr){
        uint32_t len=0;
        if(!read_size(data,end,len)){
            return -1;
        }
        out.push_back(std::string());
        if(!read_str(data,end,len,out.back())){
            return -1;
        }
    }
        if(data!=end){
            return -1; //trailing garbage
        }
        return 0;
    }



static uint64_t str_hash(const uint8_t *data, size_t len) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}


static void do_get(std::vector<std::string> &cmd, struct Buffer &out) {
    // a dummy `Entry` just for the lookup
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node) {
        return out_nil(out);
    }
    // copy the value
    const std::string &val = container_of(node, Entry, node)->val;
    assert(val.size() <= k_max_msg);
    return out_str(out, val.data(), val.size()); 
}

static void do_set(std::vector<std::string> &cmd, struct Buffer &out) {

    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);

    if(node){
        container_of(node,Entry,node)->val.swap(cmd[2]);
        return out_nil(out);
    }
    else{

    Entry *ent=new Entry();
    ent->key=key.key;
    ent->val.swap(cmd[2]);
    ent->node.hcode = key.node.hcode;
    hm_insert(&g_data.db, &ent->node);
    return out_nil(out);
}  
}

static void do_del(std::vector<std::string> &cmd, struct Buffer &out) {
    // a dummy `Entry` just for the lookup
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable delete
    HNode *node = hm_delete(&g_data.db, &key.node, &entry_eq);
    if (node) { // deallocate the pair
        delete container_of(node, Entry, node);
    }
    return out_int(out,node?1:0);
}



static void do_request(std::vector<std::string> &cmd, struct Buffer &out) {
    if (cmd.size() == 2 && cmd[0] == "get") {
        do_get(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        do_set(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        do_del(cmd, out);
    } else if (cmd.size() == 1 && cmd[0] == "keys") {
        return do_keys(cmd, out);
    } else {
        return out_err(out, ERR_UNKNOWN, "unknown command.");    // unrecognized command
    }
}


static void make_response(const Response &resp, struct Buffer &out) {
    uint32_t resp_len = 4 + (uint32_t)resp.data.size();
    buf_append(out, (const uint8_t *)&resp_len, 4);
    buf_append(out, (const uint8_t *)&resp.status, 4);
    buf_append(out, resp.data.data(), resp.data.size());
}


// remove from the front
static void buf_consume(struct Buffer &buf, size_t n) {
    assert(n <= buf_size(buf));
    buf.data_begin += n;
    if (buf.data_begin == buf.data_end) {   // empty: reuse the whole buffer
        buf.data_begin = buf.data_end = buf.buffer_begin;
    }
}

static void fd_set_nb(int fd) { // set the file descripint fd=socket(AF_INET,SOCK_STREAM,0);tor fd to non-blocking mode
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
} 


// process 1 request if there is enough data
static bool try_one_request(conn *c) {
    // 3. Try to parse the accumulated buffer.
    // Protocol: message header
    if (buf_size(c->incoming_buffer) < 4) {
        return false;   // want read
    }
    uint32_t len = 0;
    memcpy(&len, c->incoming_buffer.data_begin, 4);
    if (len > k_max_msg) {  // protocol error
        c->want_close = true;
        return false;   // want close
    }
    // Protocol: message body
    if (4 + len > buf_size(c->incoming_buffer)) {
        return false;   // want read
    }
    const uint8_t *request = &c->incoming_buffer.data_begin[4];
    
    
    std::vector<std::string> cmd;
    if(parse_req(request,len,cmd)<0){
        c->want_close=true;
        return false; //error
    }
    
    // 4. Process the parsed message.
    size_t header_pos=0;
    response_begin(c->outgoing_buffer, &header_pos);
    do_request(cmd, c->outgoing_buffer);
    response_end(c->outgoing_buffer, header_pos);
    
    
    // 5. Remove the message from `c::incoming`.
    buf_consume(c->incoming_buffer, 4 + len);
    return true;        // success
}


static struct conn* handle_accept(int fd){
    struct sockaddr_in client_addr={};
    socklen_t len=sizeof(client_addr);
    int connfd=accept(fd,(struct sockaddr*)&client_addr,&len);
    if(connfd<0){
        return NULL;
    }
    fd_set_nb(connfd);
    conn *c=new conn();
    c->fd=connfd;
    c->want_read=true;
    
    return c;

}

static void handle_write(conn *c) {
    assert(buf_size(c->outgoing_buffer) > 0);
    ssize_t rv = write(c->fd, c->outgoing_buffer.data_begin, buf_size(c->outgoing_buffer));

    if(rv<0&&errno==EAGAIN){
        return; // try again later
    }

    if (rv < 0) {
        c->want_close = true;    // error handling
        return;
    }
    // remove written data from `outgoing`
    buf_consume(c->outgoing_buffer, (size_t)rv);
    
    if (buf_size(c->outgoing_buffer) == 0) {   // all data written
        c->want_read = true;
        c->want_write = false;
        return;
    } // else: want write
    return;
}


static void handle_read(conn *c) {
    // 1. Do a non-blocking read.
    uint8_t buf[64 * 1024];
    ssize_t rv = read(c->fd, buf, sizeof(buf));
    if(rv < 0 && errno == EAGAIN) {
        return; // try again later
    }
    if (rv <= 0) {  // handle IO error (rv < 0) or EOF (rv == 0)
        c->want_close = true;
        return;
    }
    // 2. Add new data to the `Conn::incoming` buffer.
    buf_append(c->incoming_buffer, buf, (size_t)rv);
    // 3. Try to parse the accumulated buffer.
    // 4. Process the parsed message.
    // 5. Remove the message from `Conn::incoming`.
    while(try_one_request(c)){}// keep processing in case multiple requests are sent in one read
    
    if (buf_size(c->outgoing_buffer) > 0) {    // has a response
        c->want_read = false;
        c->want_write = true;
        return handle_write(c);  // try to write immediately
    }   // else: want read
}





int main(){
signal(SIGPIPE, SIG_IGN); // ignore SIGPIPE to prevent crashes on writing to closed sockets

int fd=socket(AF_INET,SOCK_STREAM,0);
if(fd<0){
    die("socket");
}
int val=1;


setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&val,sizeof(val));

struct sockaddr_in addr={};
addr.sin_family=AF_INET;
addr.sin_port=htons(8080);
addr.sin_addr.s_addr=htonl(0);
int rv=bind(fd, (const struct sockaddr *)&addr,sizeof(addr));
if(rv){
    die("bind");
}

rv=listen(fd,SOMAXCONN);
if(rv){
    die("listen");
}

fd_set_nb(fd); // set the listening socket to non-blocking mode

std::vector<struct conn *> fd2conn;// a vector to map file descriptors to connection objects

std::vector<struct pollfd> poll_args; // a vector to hold pollfd structures for poll()

// Event loop
while(true){ 
    poll_args.clear();
    // Add listening socket to poll_args
    struct pollfd pfd={fd, POLLIN, 0};
    //prepare poll_args for poll() call
    poll_args.push_back(pfd);
    for(struct conn *c:fd2conn){
        if(!c){
            continue;
        }
        struct pollfd pfd={c->fd,POLLERR,0};
        if(c->want_read){
            pfd.events|=POLLIN;
        }
        if(c->want_write){
            pfd.events|=POLLOUT;
        }
        poll_args.push_back(pfd);
    }
    // Call poll() to wait for events on the sockets
    int rv=poll(poll_args.data(),(nfds_t)poll_args.size(),-1);
    if(rv<0&&errno==EINTR){
        continue; // retry if interrupted by signal
    }
    if(rv<0){
        die("poll");
    }

    if(poll_args[0].revents){
        if(struct conn *c=handle_accept(fd)){
            if(fd2conn.size()<=(size_t)c->fd){
                fd2conn.resize(c->fd+1);
            }
            fd2conn[c->fd]=c;
        }
    }
    for (size_t i = 1; i < poll_args.size(); ++i) { // note: skip the 1st
            uint32_t ready = poll_args[i].revents;
            if (!ready) {
                continue;   // no events
            }
            struct conn *c = fd2conn[poll_args[i].fd];
            if (ready & POLLIN) {
                handle_read(c);  // application logic
            }
            if (ready & POLLOUT) {
                handle_write(c); // application logic
            }
            if ((ready & (POLLERR | POLLHUP)) || c->want_close) {
                (void)close(c->fd);
                fd2conn[c->fd] = NULL;
                delete c;
            }
        }
        
    }
}

