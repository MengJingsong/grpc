#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <getopt.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <fstream>


#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include "benchmark.grpc.pb.h"


using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using benchmark::BENCHMARK;
using benchmark::Chunk;

class ServerNode;

void ThreadRunThis(ServerNode* server, int threadIndex);

int ThreadGetCQIndex(int threadIndex);

size_t m_num_threads;
size_t m_num_CQ;
std::string addr;

class ServerNode final {

    friend void ThreadRunThis(ServerNode* server, int threadIndex);

    public:
        bool Start(std::string addr) {
            std::string server_address(addr);

            ServerBuilder builder;
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            builder.RegisterService(&m_service);

            for (size_t i = 0; i < m_num_CQ; i++) m_cqs.push_back(builder.AddCompletionQueue()); 

            m_server = builder.BuildAndStart();

            if (!m_server) {
                std::cout << "failed to start grpc server" << std::endl;
                return false;
            }

            std::cout << "Server listening on " << server_address << std::endl;

            for (size_t i = 0; i < m_num_threads; i++) m_threads.emplace_back(std::thread(ThreadRunThis, this, i));

            for (size_t i = 0; i < m_num_threads; i++) m_threads[i].join();

            return true;
        }

    private:
        enum ServiceType {HELLO, LATENCY, THROUGHPUT};

        void HandleRpcs(int threadIndex) {
            int CQIndex = ThreadGetCQIndex(threadIndex);
            ServerCompletionQueue* cq = m_cqs[CQIndex].get();
            new CallData(&m_service, cq, threadIndex, HELLO);
            new CallData(&m_service, cq, threadIndex, LATENCY);
            new CallData(&m_service, cq, threadIndex, THROUGHPUT);

            auto deadline = gpr_time_from_millis(0, GPR_TIMESPAN);
            void* tag = nullptr;
            bool ok;
            grpc::CompletionQueue::NextStatus status;
            while((status = cq->AsyncNext(&tag, &ok, deadline)) != grpc::CompletionQueue::SHUTDOWN) {
                if (status == grpc::CompletionQueue::GOT_EVENT) {
                    if (ok) {
                        // printf("thread %d is in HandleRpcs, GOT_EVENT, ok\n", threadIndex);
                        static_cast<CallData*>(tag)->Proceed();
                    } else {
                        printf("thread %d is in HandleRpcs, GOT_EVENT, not ok\n", threadIndex);
                    }
                } else if (status == grpc::CompletionQueue::TIMEOUT) {
                    // printf("thread %d is in HandleRpcs, TIMEOUT\n", threadIndex);
                }
            }
            printf("thread %d is in HandleRpcs, SHUTDOWN\n", threadIndex);
        }

        class CallData {
            public:
                CallData(BENCHMARK::AsyncService* service,
                         ServerCompletionQueue* cq,
                         int threadIndex,
                         ServiceType type)
                         : service_(service),
                           cq_(cq),
                           status_(CREATE),
                           responder_(&ctx_),
                           threadIndex_(threadIndex),
                           type_(type) {
                               Proceed();
                           }
                
                void Proceed() {
                    if (status_ == CREATE) {
                        // printf("thread %d is in Proceed, CREATE\n", threadIndex_);
                        status_ = PROCESS;
                        switch (type_) {
                            case HELLO:
                                service_->RequesthelloThere(&ctx_, &request_, &responder_, cq_, cq_, this);
                                break;
                            case LATENCY:
                                service_->RequestlatencyTest(&ctx_, &request_, &responder_, cq_, cq_, this);
                                break;
                            case THROUGHPUT:
                                service_->RequestthroughputTest(&ctx_, &request_, &responder_, cq_, cq_, this);
                                break;
                        }
                    } else if (status_ == PROCESS) {
                        // printf("thread %d is in Proceed, PROCESS, %d\n", threadIndex_, type_);
                        new CallData(service_, cq_, threadIndex_, type_);
                        switch (type_) {
                            case HELLO:
                                ProceedhelloThere();
                                break;
                            case LATENCY:
                                ProceedlatencyTest();
                                break;
                            case THROUGHPUT:
                                ProceedThroughputTest();
                                break;
                        }
                        status_ = FINISH;
                    } else {
                        // printf("thread %d is in Proceed, SHUTDOWN\n", threadIndex_);
                        
                        GPR_ASSERT(status_ == FINISH);
                        delete this;
                    }
                }

                void ProceedhelloThere(){
                    // printf("thread %d is in ProceedhelloThere\n", threadIndex_);
                    responder_.Finish(reply_, Status::OK, this);
                }

                void ProceedlatencyTest(){
                    // printf("thread %d is in ProceedlatencyTest\n", threadIndex_);
                    reply_.set_data(request_.data().c_str(), request_.data().length());
                    responder_.Finish(reply_, Status::OK, this);
                }

                void ProceedThroughputTest(){
                    // printf("thread %d is in ProceedThroughputTest\n", threadIndex_);
                    responder_.Finish(reply_, Status::OK, this);
                }

            private:
                BENCHMARK::AsyncService* service_;
                ServerCompletionQueue* cq_;
                ServerContext ctx_;
                int threadIndex_;
                Chunk request_;
                Chunk reply_;
                ServerAsyncResponseWriter<Chunk> responder_;

                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;
                ServiceType type_;
        };

        BENCHMARK::AsyncService m_service;
        std::vector<std::unique_ptr<ServerCompletionQueue>> m_cqs;
        std::unique_ptr<Server> m_server;
        std::vector<std::thread> m_threads;

};

void ThreadRunThis(ServerNode* server, int threadIndex) {
    // char[16] = "server thread"
    // pthread_setname_np(pthread_self(), )
    std::string threadName = "thread " +  std::to_string(threadIndex);
    pthread_setname_np(pthread_self(), threadName.c_str());
    printf("thread %d starts running \n", threadIndex);
    server->HandleRpcs(threadIndex);
    printf("thread %d finished \n", threadIndex);
}

int ThreadGetCQIndex(int threadIndex) {
    return threadIndex % m_num_CQ;
}

int main(int argc, char** argv) {

    int flag;
    while((flag = getopt(argc, argv, "l:t:q:")) != -1) {
        switch (flag) {
            case 'l':
                addr = std::string(optarg);
                break;
            case 't':
                m_num_CQ = atoi(optarg);
                break;
            case 'q':
                m_num_threads = atoi(optarg);
                break;
            default:
                printf("command line arguments wrong!!!\n");
                exit(EXIT_FAILURE);
        }
    }

    ServerNode server;
    server.Start(addr);
}