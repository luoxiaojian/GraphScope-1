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

std::string parse_tokens(const std::vector<std::string>& tokens) {
  if (tokens[0] == "ic2") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id, max_date;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;
    std::istringstream iss2(tokens[2]);
    iss2 >> max_date;

    encoder.put_long(person_id);
    encoder.put_long(max_date);
    encoder.put_byte(static_cast<uint8_t>(2));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic3") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id, start_date;
    int duration_days;
    std::string countryx, countryy;

    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;
    std::istringstream iss2(tokens[2]);
    iss2 >> start_date;
    std::istringstream iss3(tokens[3]);
    iss3 >> duration_days;
    std::istringstream iss4(tokens[4]);
    iss4 >> countryx;
    std::istringstream iss5(tokens[5]);
    iss5 >> countryy;

    encoder.put_long(person_id);
    encoder.put_string(countryx);
    encoder.put_string(countryy);
    encoder.put_long(start_date);
    encoder.put_int(duration_days);
    encoder.put_byte(static_cast<uint8_t>(3));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic4") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id, start_date;
    int duration_days;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;
    std::istringstream iss2(tokens[2]);
    iss2 >> start_date;
    std::istringstream iss3(tokens[3]);
    iss3 >> duration_days;

    encoder.put_long(person_id);
    encoder.put_long(start_date);
    encoder.put_int(duration_days);
    encoder.put_byte(static_cast<uint8_t>(4));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic5") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id, min_date;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;
    std::istringstream iss2(tokens[2]);
    iss2 >> min_date;

    encoder.put_long(person_id);
    encoder.put_long(min_date);
    encoder.put_byte(static_cast<uint8_t>(5));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic6") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id;
    std::string tagname;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;
    std::istringstream iss2(tokens[2]);
    iss2 >> tagname;

    encoder.put_long(person_id);
    encoder.put_string(tagname);
    encoder.put_byte(static_cast<uint8_t>(6));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic7") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;

    encoder.put_long(person_id);
    encoder.put_byte(static_cast<uint8_t>(7));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic8") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;

    encoder.put_long(person_id);
    encoder.put_byte(static_cast<uint8_t>(8));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic9") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id;
    int64_t max_date;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;
    std::istringstream iss2(tokens[2]);
    iss2 >> max_date;

    encoder.put_long(person_id);
    encoder.put_long(max_date);
    encoder.put_byte(static_cast<uint8_t>(9));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic10") {
  } else if (tokens[0] == "ic11") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id;
    std::string country;
    int work_from;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;
    std::istringstream iss2(tokens[2]);
    iss2 >> country;
    std::istringstream iss3(tokens[3]);
    iss3 >> work_from;

    encoder.put_long(person_id);
    encoder.put_string(country);
    encoder.put_int(work_from);
    encoder.put_byte(static_cast<uint8_t>(11));

    return std::string(buf.data(), buf.size());
  } else if (tokens[0] == "ic12") {
    std::vector<char> buf;
    gs::Encoder encoder(buf);

    int64_t person_id;
    std::string name;
    std::istringstream iss1(tokens[1]);
    iss1 >> person_id;

    std::istringstream iss2(tokens[2]);
    iss2 >> name;

    encoder.put_long(person_id);
    encoder.put_string(name);
    encoder.put_byte(static_cast<uint8_t>(12));

    return std::string(buf.data(), buf.size());
  } else {
    return "";
  }
}

std::vector<std::string> parse_params_file(const std::string& file) {
  std::ifstream fin(file);
  std::vector<std::string> ret;
  if (!fin.is_open()) {
    LOG(ERROR) << "open " << file << " failed...";
    return ret;
  }

  std::string line;
  while (std::getline(fin, line)) {
    std::stringstream ss(line);
    std::string token;
    std::vector<std::string> tokens;

    while (std::getline(ss, token, '|')) {
      tokens.push_back(token);
    }

    ret.push_back(parse_tokens(tokens));
  }

  return ret;
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")("version,v",
                                                     "Display version")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
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

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  gs::GraphDBConfig config(schema, data_path, 1);
  config.memory_level = 2;
  db.Open(config);

  t0 += grape::GetCurrentTime();

  std::string input_path = vm["input-params"].as<std::string>();
  std::string output_path = vm["output-path"].as<std::string>();

  std::vector<std::string> params = parse_params_file(input_path);
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
