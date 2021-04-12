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

class ServerNode final {
    public:
        ~ServerNode() {
            m_server->Shutdown();
            m_cq->Shutdown();
        }

        void Run(std::string server_address) {
            ServerBuilder builder;
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            builder.RegisterService(&m_service);
            m_cq = builder.AddCompletionQueue();
            m_server = builder.BuildAndStart();
            std::cout << "Server listening on " << server_address << std::endl;
            HandleRpcs();
        }

    private:
        enum ServiceType {HELLO, LATENCY, THROUGHPUT};

        void HandleRpcs() {
            new CallData(&m_service, m_cq.get(), HELLO);
            new CallData(&m_service, m_cq.get(), LATENCY);
            new CallData(&m_service, m_cq.get(), THROUGHPUT);

            auto deadline = gpr_time_from_millis(0, GPR_TIMESPAN);
            void* tag = nullptr;
            bool ok;
            grpc::CompletionQueue::NextStatus status;
            while((status = m_cq.get()->AsyncNext(&tag, &ok, deadline)) != grpc::CompletionQueue::SHUTDOWN) {
                if (status == grpc::CompletionQueue::GOT_EVENT) {
                    if (ok) {
                        // printf("In HandleRpcs, GOT_EVENT, ok\n");
                        static_cast<CallData*>(tag)->Proceed();
                    } else {
                        printf("In HandleRpcs, GOT_EVENT, not ok\n");
                    }
                } else if (status == grpc::CompletionQueue::TIMEOUT) {
                }
            }
            printf("In HandleRpcs, SHUTDOWN\n");
        }

        class CallData {
            public:
                CallData(BENCHMARK::AsyncService* service,
                         ServerCompletionQueue* cq,
                         ServiceType type)
                         : service_(service),
                           cq_(cq),
                           status_(CREATE),
                           responder_(&ctx_),
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
                        new CallData(service_, cq_, type_);
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
                Chunk request_;
                Chunk reply_;
                ServerAsyncResponseWriter<Chunk> responder_;

                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;
                ServiceType type_;
        };

        std::unique_ptr<ServerCompletionQueue> m_cq;
        BENCHMARK::AsyncService m_service;
        std::unique_ptr<Server> m_server;
};

int main(int argc, char** argv) {

    std::string server_address = std::string(argv[1]);

    ServerNode server;
    server.Run(server_address);

    return 0;
}