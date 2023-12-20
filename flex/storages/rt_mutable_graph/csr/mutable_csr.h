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

#ifndef GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_MUTABLE_CSR_H_
#define GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_MUTABLE_CSR_H_

#include "flex/storages/rt_mutable_graph/csr/adj_list.h"
#include "flex/storages/rt_mutable_graph/csr/csr_base.h"
#include "flex/storages/rt_mutable_graph/csr/nbr.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

template <typename EDATA_T>
class MutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using adjlist_t = MutableAdjlist<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableCsr() : locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree) override {
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", false);
    adj_lists_.resize(vnum);

    locks_ = new grape::SpinLock[vnum];

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += d;
    }
    nbr_list_.open(work_dir + "/" + name + ".nbr", false);
    nbr_list_.resize(edge_num);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      adj_lists_[i].init(ptr, deg, 0);
      ptr += deg;
    }
    return edge_num;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    mmap_array<int> degree_list;
    if (snapshot_dir != "") {
      degree_list.open(snapshot_dir + "/" + name + ".deg", true);
      nbr_list_.open(snapshot_dir + "/" + name + ".nbr", true);
    }
    nbr_list_.touch(work_dir + "/" + name + ".nbr");
    adj_lists_.open(work_dir + "/" + name + ".adj", false);

    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    std::string degree_list_idx_file = snapshot_dir + "/" + name + ".deg.idx";
    if (std::filesystem::exists(degree_list_idx_file)) {
      mmap_array<uint64_t> degree_list_idx;
      degree_list_idx.open(degree_list_idx_file, true);
      uint64_t chunk_size = degree_list_idx[0];
      uint64_t chunk_num = degree_list_idx.size();
      int concurrency = std::thread::hardware_concurrency();
      std::vector<std::thread> threads;
      std::atomic<uint64_t> chunk_i(0);
      for (int i = 0; i < concurrency; ++i) {
        threads.emplace_back([&]() {
          while (true) {
            uint64_t cur_chunk = chunk_i.fetch_add(1);
            if (cur_chunk >= chunk_num) {
              break;
            }
            uint64_t begin = cur_chunk * chunk_size;
            uint64_t end = std::min(begin + chunk_size, degree_list.size());

            uint64_t offset = cur_chunk == 0 ? 0 : degree_list_idx[cur_chunk];
            nbr_t* ptr = nbr_list_.data() + offset;
            while (begin < end) {
              int degree = degree_list[begin];
              adj_lists_[begin].init(ptr, degree, degree);
              ptr += degree;
              ++begin;
            }
          }
        });
      }
      for (auto& thrd : threads) {
        thrd.join();
      }

    } else {
      nbr_t* ptr = nbr_list_.data();
      for (size_t i = 0; i < degree_list.size(); ++i) {
        int degree = degree_list[i];
        adj_lists_[i].init(ptr, degree, degree);
        ptr += degree;
      }
    }
  }

  void warmup(int thread_num) const override {
    size_t vnum = adj_lists_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    const size_t chunk = 4096;
    std::atomic<size_t> output(0);
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);

          if (begin == end) {
            break;
          }

          while (begin < end) {
            auto adj_list = get_edges(begin);
            for (auto& nbr : adj_list) {
              ret += nbr.neighbor;
            }
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {
    size_t vnum = adj_lists_.size();
    bool reuse_nbr_list = true;
    mmap_array<int> degree_list;
    degree_list.open(new_spanshot_dir + "/" + name + ".deg", false);
    degree_list.resize(vnum);
    size_t offset = 0;
    for (size_t i = 0; i < vnum; ++i) {
      if (adj_lists_[i].size() != 0) {
        if (!(adj_lists_[i].data() == nbr_list_.data() + offset &&
              offset < nbr_list_.size())) {
          reuse_nbr_list = false;
        }
      }
      degree_list[i] = adj_lists_[i].size();
      offset += degree_list[i];
    }

    {
      size_t input_size = degree_list.size();
      std::string degree_list_idx_file =
          new_spanshot_dir + "/" + name + ".deg.idx";
      if (input_size > 128 * 1024 &&
          !std::filesystem::exists(degree_list_idx_file)) {
        mmap_array<uint64_t> degree_list_idx;
        degree_list_idx.open(degree_list_idx_file, false);

        const uint64_t chunk_num = 128;
        degree_list_idx.resize(chunk_num);

        uint64_t chunk_size = (input_size + chunk_num - 1) / chunk_num;
        uint64_t sum = 0;
        for (size_t i = 0; i < input_size; ++i) {
          // sum += degree_list[i];
          if (i % chunk_size == 0) {
            degree_list_idx[i / chunk_size] = sum;
          }
          sum += degree_list[i];
        }
        degree_list_idx[0] = chunk_size;
      }
    }

    if (reuse_nbr_list && !nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_spanshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fout =
          fopen((new_spanshot_dir + "/" + name + ".nbr").c_str(), "wb");
      for (size_t i = 0; i < vnum; ++i) {
        fwrite(adj_lists_[i].data(), sizeof(nbr_t), adj_lists_[i].size(), fout);
      }
      fflush(fout);
      fclose(fout);
    }
  }

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        adj_lists_[k].init(NULL, 0, 0);
      }
      delete[] locks_;
      locks_ = new grape::SpinLock[vnum];
    } else {
      adj_lists_.resize(vnum);
    }
  }

  size_t size() const override { return adj_lists_.size(); }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts, alloc);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator& alloc) override {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, alloc);
    locks_[src].unlock();
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges();
  }
  mut_slice_t get_edges_mut(vid_t i) override {
    return adj_lists_[i].get_edges_mut();
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
  grape::SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
};

template <>
class MutableCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
 public:
  using nbr_t = MutableNbr<size_t>;
  using adjlist_t = MutableAdjlist<std::string_view>;
  using slice_t = MutableNbrSlice<std::string_view>;
  using mut_slice_t = MutableNbrSliceMut<std::string_view>;

  MutableCsr(StringColumn& column, std::atomic<size_t>& column_idx)
      : column_(column), column_idx_(column_idx), locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree) override {
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", false);
    adj_lists_.resize(vnum);

    locks_ = new grape::SpinLock[vnum];

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += d;
    }
    nbr_list_.open(work_dir + "/" + name + ".nbr", false);
    nbr_list_.resize(edge_num);

    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      adj_lists_[i].init(ptr, deg, 0);
      ptr += deg;
    }
    return edge_num;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    mmap_array<int> degree_list;
    if (snapshot_dir != "") {
      degree_list.open(snapshot_dir + "/" + name + ".deg", true);
      nbr_list_.open(snapshot_dir + "/" + name + ".nbr", true);
    }
    nbr_list_.touch(work_dir + "/" + name + ".nbr");
    adj_lists_.open(work_dir + "/" + name + ".adj", false);

    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    std::string degree_list_idx_file = snapshot_dir + "/" + name + ".deg.idx";
    if (std::filesystem::exists(degree_list_idx_file)) {
      mmap_array<uint64_t> degree_list_idx;
      degree_list_idx.open(degree_list_idx_file, true);
      uint64_t chunk_size = degree_list_idx[0];
      uint64_t chunk_num = degree_list_idx.size();
      int concurrency = std::thread::hardware_concurrency();
      std::vector<std::thread> threads;
      std::atomic<uint64_t> chunk_i(0);
      for (int i = 0; i < concurrency; ++i) {
        threads.emplace_back([&]() {
          while (true) {
            uint64_t cur_chunk = chunk_i.fetch_add(1);
            if (cur_chunk >= chunk_num) {
              break;
            }
            uint64_t begin = cur_chunk * chunk_size;
            uint64_t end = std::min(begin + chunk_size, degree_list.size());

            uint64_t offset = cur_chunk == 0 ? 0 : degree_list_idx[cur_chunk];
            nbr_t* ptr = nbr_list_.data() + offset;
            while (begin < end) {
              int degree = degree_list[begin];
              adj_lists_[begin].init(ptr, degree, degree);
              ptr += degree;
              ++begin;
            }
          }
        });
      }
      for (auto& thrd : threads) {
        thrd.join();
      }

    } else {
      nbr_t* ptr = nbr_list_.data();
      for (size_t i = 0; i < degree_list.size(); ++i) {
        int degree = degree_list[i];
        adj_lists_[i].init(ptr, degree, degree);
        ptr += degree;
      }
    }
  }

  void warmup(int thread_num) const override {
    size_t vnum = adj_lists_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    const size_t chunk = 4096;
    std::atomic<size_t> output(0);
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);

          if (begin == end) {
            break;
          }

          while (begin < end) {
            auto adj_list = get_edges(begin);
            for (auto& nbr : adj_list) {
              ret += nbr.get_neighbor();
            }
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {
    size_t vnum = adj_lists_.size();
    bool reuse_nbr_list = true;
    mmap_array<int> degree_list;
    degree_list.open(new_spanshot_dir + "/" + name + ".deg", false);
    degree_list.resize(vnum);
    size_t offset = 0;
    for (size_t i = 0; i < vnum; ++i) {
      if (adj_lists_[i].size() != 0) {
        if (!(adj_lists_[i].data() == nbr_list_.data() + offset &&
              offset < nbr_list_.size())) {
          reuse_nbr_list = false;
        }
      }
      degree_list[i] = adj_lists_[i].size();
      offset += degree_list[i];
    }

    {
      size_t input_size = degree_list.size();
      std::string degree_list_idx_file =
          new_spanshot_dir + "/" + name + ".deg.idx";
      if (input_size > 128 * 1024 &&
          !std::filesystem::exists(degree_list_idx_file)) {
        mmap_array<uint64_t> degree_list_idx;
        degree_list_idx.open(degree_list_idx_file, false);

        const uint64_t chunk_num = 128;
        degree_list_idx.resize(chunk_num);

        uint64_t chunk_size = (input_size + chunk_num - 1) / chunk_num;
        uint64_t sum = 0;
        for (size_t i = 0; i < input_size; ++i) {
          // sum += degree_list[i];
          if (i % chunk_size == 0) {
            degree_list_idx[i / chunk_size] = sum;
          }
          sum += degree_list[i];
        }
        degree_list_idx[0] = chunk_size;
      }
    }

    if (reuse_nbr_list && !nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_spanshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fout =
          fopen((new_spanshot_dir + "/" + name + ".nbr").c_str(), "wb");
      for (size_t i = 0; i < vnum; ++i) {
        fwrite(adj_lists_[i].data(), sizeof(nbr_t), adj_lists_[i].size(), fout);
      }
      fflush(fout);
      fclose(fout);
    }
  }

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        adj_lists_[k].init(NULL, 0, 0);
      }
      delete[] locks_;
      locks_ = new grape::SpinLock[vnum];
    } else {
      adj_lists_.resize(vnum);
    }
  }

  size_t size() const override { return adj_lists_.size(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    LOG(FATAL) << "not implemented\n";
  }

  void put_edge(vid_t src, vid_t dst, size_t data, timestamp_t ts,
                Allocator& alloc) {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, alloc);
    locks_[src].unlock();
  }
  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {
    put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, const size_t& data,
                                 timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }

  int degree(vid_t i) const { return adj_lists_[i].size(); }

  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges(column_);
  }
  mut_slice_t get_edges_mut(vid_t i) {
    return adj_lists_[i].get_edges_mut(column_);
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    auto row_id = column_idx_.load();
    put_edge(src, dst, row_id - 1, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    std::string_view sw;
    arc >> sw;
    auto row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, sw);
    put_edge(src, dst, row_id, ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<std::string_view>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<std::string_view>(get_edges(v));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<std::string_view>>(
        get_edges_mut(v));
  }

 private:
  grape::SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
  StringColumn& column_;
  std::atomic<size_t>& column_idx_;
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
    nbr_list_.resize(vnum);
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
    return vnum;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    if (!std::filesystem::exists(work_dir + "/" + name + ".snbr")) {
      copy_file(snapshot_dir + "/" + name + ".snbr",
                work_dir + "/" + name + ".snbr");
    }
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(!nbr_list_.filename().empty() &&
           std::filesystem::exists(nbr_list_.filename()));
    assert(!nbr_list_.read_only());
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".snbr");
  }

  void resize(vid_t vnum) override {
    if (vnum > nbr_list_.size()) {
      size_t old_size = nbr_list_.size();
      nbr_list_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts, alloc);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator&) override {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  const nbr_t& get_edge(vid_t i) const { return nbr_list_[i]; }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

  void warmup(int thread_num) const override {
    size_t vnum = nbr_list_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    std::atomic<size_t> output(0);
    const size_t chunk = 4096;
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);
          if (begin == end) {
            break;
          }
          while (begin < end) {
            auto& nbr = nbr_list_[begin];
            ret += nbr.neighbor;
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

template <>
class SingleMutableCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
 public:
  using nbr_t = MutableNbr<size_t>;
  using slice_t = MutableNbrSlice<std::string_view>;
  using mut_slice_t = MutableNbrSliceMut<std::string_view>;

  SingleMutableCsr(StringColumn& column, std::atomic<size_t>& column_idx)
      : column_(column), column_idx_(column_idx) {}
  ~SingleMutableCsr() {}

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
    nbr_list_.resize(vnum);
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
    return vnum;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    if (!std::filesystem::exists(work_dir + "/" + name + ".snbr")) {
      copy_file(snapshot_dir + "/" + name + ".snbr",
                work_dir + "/" + name + ".snbr");
    }
    nbr_list_.open(work_dir + "/" + name + ".snbr", false);
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(!nbr_list_.filename().empty() &&
           std::filesystem::exists(nbr_list_.filename()));
    assert(!nbr_list_.read_only());
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".snbr");
  }

  void resize(vid_t vnum) override {
    if (vnum > nbr_list_.size()) {
      size_t old_size = nbr_list_.size();
      nbr_list_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator& alloc) override {
    LOG(FATAL) << "not implemented\n";
  }

  void put_edge(vid_t src, vid_t dst, size_t data, timestamp_t ts, Allocator&) {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  slice_t get_edges(vid_t i) const override {
    slice_t ret(column_);
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret(column_);
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  const MutableNbr<std::string_view>& get_edge(vid_t i) const {
    nbr_.neighbor = nbr_list_[i].neighbor;
    nbr_.timestamp.store(nbr_list_[i].timestamp.load());
    nbr_.data = column_.get_view(nbr_list_[i].data);
    return nbr_;
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator& alloc) override {
    auto row_id = column_idx_.load();
    put_edge(src, dst, row_id - 1, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, Allocator& alloc) override {
    std::string_view sw;
    arc >> sw;

    auto row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, sw);
    put_edge(src, dst, row_id, ts, alloc);
  }
  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {
    put_edge(src, dst, index, ts, alloc);
  }

  void batch_put_edge_with_index(vid_t src, vid_t dst, const size_t& data,
                                 timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<std::string_view>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<std::string_view>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<std::string_view>>(
        get_edges_mut(v));
  }

  void warmup(int thread_num) const override {
    size_t vnum = nbr_list_.size();
    std::vector<std::thread> threads;
    std::atomic<size_t> v_i(0);
    std::atomic<size_t> output(0);
    const size_t chunk = 4096;
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([&]() {
        size_t ret = 0;
        while (true) {
          size_t begin = std::min(v_i.fetch_add(chunk), vnum);
          size_t end = std::min(begin + chunk, vnum);
          if (begin == end) {
            break;
          }
          while (begin < end) {
            auto& nbr = nbr_list_[begin];
            ret += nbr.neighbor;
            ++begin;
          }
        }
        output.fetch_add(ret);
      });
    }
    for (auto& thrd : threads) {
      thrd.join();
    }
    (void) output.load();
  }

 private:
  mmap_array<nbr_t> nbr_list_;
  mutable MutableNbr<std::string_view> nbr_;
  std::atomic<size_t>& column_idx_;
  StringColumn& column_;
};

template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
  using slice_t = MutableNbrSlice<EDATA_T>;

 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree) override {
    return 0;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {}

  void warmup(int thread_num) const override {}

  void resize(vid_t vnum) override {}

  size_t size() const override { return 0; }

  slice_t get_edges(vid_t i) const override { return slice_t::empty(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator&) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                Allocator&) override {}
  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator&) override {
    EDATA_T value;
    arc >> value;
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts, Allocator&) override {}

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(
        MutableNbrSliceMut<EDATA_T>::empty());
  }
};

template <>
class EmptyCsr<std::string_view>
    : public TypedMutableCsrBase<std::string_view> {
  using slice_t = MutableNbrSlice<std::string_view>;

 public:
  EmptyCsr(StringColumn& column, std::atomic<size_t>& column_idx)
      : column_(column), column_idx_(column_idx) {}
  ~EmptyCsr() = default;

  size_t batch_init(const std::string& name, const std::string& work_dir,
                    const std::vector<int>& degree) override {
    return 0;
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {}

  void warmup(int thread_num) const override {}

  void resize(vid_t vnum) override {}

  size_t size() const override { return 0; }

  slice_t get_edges(vid_t i) const override { return slice_t::empty(column_); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        Allocator&) override {
    LOG(FATAL) << "not implemented\n";
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   Allocator&) override {}

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts, Allocator&) override {
    std::string_view sw;
    arc >> sw;
    auto row_id = column_idx_.fetch_add(1);
    column_.set_value(row_id, sw);
  }
  void put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts,
                           Allocator& alloc) override {}
  void batch_put_edge_with_index(vid_t src, vid_t dst, const size_t& data,
                                 timestamp_t ts = 0) override {}
  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<std::string_view>>(
        MutableNbrSlice<std::string_view>::empty(column_));
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<std::string_view>(
        MutableNbrSlice<std::string_view>::empty(column_));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<std::string_view>>(
        MutableNbrSliceMut<std::string_view>::empty(column_));
  }
  std::atomic<size_t>& column_idx_;
  StringColumn& column_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FLEX_STORAGES_RT_MUTABLE_GRAPH_CSR_MUTABLE_CSR_H_