/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "grape/util.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

#include <boost/program_options.hpp>

#include <glog/logging.h>

namespace bpo = boost::program_options;

std::string read_pb(const std::string& filename) {
  std::ifstream file(filename, std::ios::binary);

  if (!file.is_open()) {
    LOG(FATAL) << "open pb file: " << filename << " failed...";
    return "";
  }

  file.seekg(0, std::ios::end);
  size_t size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::string buffer;
  buffer.resize(size);

  file.read(&buffer[0], size);

  file.close();
  return buffer;
}

std::vector<std::string> parse_params_file(const std::string& input,
                                           const std::string& query) {
  std::string query_str = read_pb(query);

  std::ifstream fin(input);
  std::vector<std::string> ret;
  if (!fin.is_open()) {
    LOG(ERROR) << "open " << input << " failed...";
    return ret;
  }

  std::string line;
  std::vector<std::string> header;
  {
    std::getline(fin, line);
    std::stringstream ss(line);
    std::string token;
    while (std::getline(ss, token, '|')) {
      header.push_back(token);
    }
  }

  while (std::getline(fin, line)) {
    std::stringstream ss(line);
    std::string token;
    std::vector<std::string> tokens;

    while (std::getline(ss, token, '|')) {
      tokens.push_back(token);
    }
    CHECK_EQ(tokens.size(), header.size());

    std::vector<char> buf;
    gs::Encoder encoder(buf);
    encoder.put_string(query_str);
    for (size_t k = 0; k < header.size(); ++k) {
      encoder.put_string(header[k]);
      encoder.put_string(tokens[k]);
    }
    encoder.put_byte(1);

    ret.emplace_back(buf.data(), buf.size());
  }

  return ret;
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")("version,v",
                                                     "Display version")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "query,q", bpo::value<std::string>(), "query definition path")(
      "input-params,i", bpo::value<std::string>(), "input params file")(
      "output-path,o", bpo::value<std::string>(), "output file path")(
      "num-of-query,n", bpo::value<int>(), "num of query");
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }
  if (vm.count("version")) {
    std::cout << "GraphScope/Flex version " << FLEX_VERSION << std::endl;
    return 0;
  }

  std::string graph_schema_path = "";
  std::string data_path = "";

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  gs::GraphDBConfig config(schema, data_path, 1);
  config.memory_level = 1;
  db.Open(config);

  std::string input_path = vm["input-params"].as<std::string>();
  std::string query_path = vm["query"].as<std::string>();
  std::string output_path = vm["output-path"].as<std::string>();

  std::vector<std::string> params = parse_params_file(input_path, query_path);
  std::vector<std::vector<char>> results;
  int nq = std::numeric_limits<int>::max();
  if (vm.count("num-of-query")) {
    nq = vm["num-of-query"].as<int>();
  }

  auto& session = db.GetSession(0);
  size_t idx = 0;
  nq = std::min<int>(nq, params.size());
  params.resize(nq);

  double tq = -grape::GetCurrentTime();
  for (auto& param : params) {
    auto ret = session.Eval(param);
    if (!ret.ok()) {
      LOG(ERROR) << "query - " << idx << " failed...";
    }
    auto result = ret.value();
    results.emplace_back(std::move(result));
  }
  tq += grape::GetCurrentTime();
  LOG(INFO) << "executed " << params.size() << " queries: " << tq << " s";

  FILE* fout = fopen(output_path.c_str(), "wb");
  for (auto& result : results) {
    fwrite(result.data(), sizeof(char), result.size(), fout);
  }
  fflush(fout);
  fclose(fout);

  return 0;
}