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
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using benchmark::BENCHMARK;
using benchmark::Chunk;

class ServerNode final : public BENCHMARK::Service {
    public:
        Status helloThere(ServerContext* context, const Chunk* chunk, Chunk* result) override {
            // printf("Server (helloThere): recv hello, send OK back\n");
            return Status::OK;
        }

        Status latencyTest(ServerContext* context, const Chunk* chunk, Chunk* result) override {
            result->set_data(chunk->data().c_str(), chunk->data().length());
            // printf("Server (latencyTest): recv file %d (%d bytes), send same file back\n",
            //     ++latencyTestCount, chunk->data().length());
            return Status::OK;
        }

        Status throughputTest(ServerContext* context, const Chunk* chunk, Chunk* result) override {
            // printf("Server (throughputTest): recv file %d (%d bytes), send OK back\n",
            //     ++throughputTestCount, chunk->data().length());
            return Status::OK;
        }

    private:
        size_t latencyTestCount = 0;
        size_t throughputTestCount = 0;
};

int main (int argc, char** argv)
{
	std::string server_address = std::string(argv[1]);
  	ServerNode service;
  	ServerBuilder builder;
  	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  	builder.RegisterService(&service);
  	std::unique_ptr<Server> server(builder.BuildAndStart());
  	std::cout << "Server: listening on " << server_address << std::endl;
  	server->Wait();
	return 0;
}