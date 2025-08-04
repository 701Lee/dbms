#include "replacer/lfu_replacer.h"

#include <algorithm>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

/**
 * @brief 简单测试LFUReplacer的基本功能
 * @note lab1 计分：5 points
 */
TEST(LFUReplacerTest, SimpleTest) {
    LFUReplacer lfu_replacer(7);

    // 添加页，频率都为1
    lfu_replacer.unpin(1);
    lfu_replacer.unpin(2);
    lfu_replacer.unpin(3);
    lfu_replacer.unpin(4);
    lfu_replacer.unpin(5);
    lfu_replacer.unpin(6);

    // 增加1的访问频率到2
    lfu_replacer.unpin(1);

    EXPECT_EQ(6, lfu_replacer.Size());

    int value;
    std::vector<int> victims;

    // 获取3个victim，期望先淘汰频率为1中最早加入的元素（2、3、4）
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(lfu_replacer.victim(&value));
        victims.push_back(value);
    }

    // 不能保证顺序，所以应在这些元素中
    for (int v : victims) {
        EXPECT_TRUE(v == 2 || v == 3 || v == 4 || v == 5 || v == 6);
    }

    // Pin住4（已淘汰）和5（还在），使其不可被淘汰
    lfu_replacer.pin(4);
    lfu_replacer.pin(5);

    EXPECT_EQ(2, lfu_replacer.Size());

    // 再unpin 5（频率+1），它现在可以淘汰
    lfu_replacer.unpin(5);

    // 剩余：6(1), 5(2), 1(2) -> 按频率+LRU淘汰
    EXPECT_TRUE(lfu_replacer.victim(&value));
    EXPECT_EQ(6, value);

    EXPECT_TRUE(lfu_replacer.victim(&value));
    EXPECT_EQ(5, value);

    EXPECT_TRUE(lfu_replacer.victim(&value));
    EXPECT_EQ(1, value);
}

/**
 * @brief 大规模测试LFUReplacer的基本功能
 */
TEST(LFUReplacerTest, MixTest) {
    int result;
    int value_size = 10000;
    auto lfu_replacer = new LFUReplacer(value_size);
    std::vector<int> value(value_size);
    for (int i = 0; i < value_size; i++) {
        value[i] = i;
    }

    std::shuffle(value.begin(), value.end(), std::default_random_engine{});

    // 初始全部 unpin，一次频率为1
    for (int i = 0; i < value_size; i++) {
        lfu_replacer->unpin(value[i]);
    }

    EXPECT_EQ(value_size, lfu_replacer->Size());

    // 让777频率增加到3
    lfu_replacer->pin(777);
    lfu_replacer->unpin(777); // freq=2
    lfu_replacer->unpin(777); // freq=3

    // 让 value[0] 被淘汰（频率=1），然后再 unpin，使其重新变为 freq=1
    EXPECT_TRUE(lfu_replacer->victim(&result));
    EXPECT_EQ(value[0], result);
    lfu_replacer->unpin(value[0]);

    // value[0]=1，777=3，其余=1
    // 现在提升前半部分元素频率
    for (int i = 0; i < value_size / 2; i++) {
        if (value[i] != value[0] && value[i] != 777) {
            lfu_replacer->pin(value[i]);    // 不可淘汰
            lfu_replacer->unpin(value[i]);  // freq=2
            lfu_replacer->unpin(value[i]);  // freq=3
        }
    }

    // 构造 LFU victim 顺序：后半部分（频率1） -> 777（频率3） -> value[0]（频率1） -> 前半部分（频率3）
    std::vector<int> lfu_order;

    // 后半部分的频率为1
    for (int i = value_size / 2; i < value_size; ++i) {
        if (value[i] != value[0] && value[i] != 777) {
            lfu_order.push_back(value[i]);
        }
    }

    // 再是 value[0] 和 777
    lfu_order.push_back(value[0]);
    lfu_order.push_back(777);

    // 前半部分的高频元素
    for (int i = 0; i < value_size / 2; ++i) {
        if (value[i] != value[0] && value[i] != 777) {
            lfu_order.push_back(value[i]);
        }
    }

    for (int e : lfu_order) {
        EXPECT_TRUE(lfu_replacer->victim(&result));
        EXPECT_EQ(e, result);
    }

    EXPECT_EQ(value_size - lfu_order.size(), lfu_replacer->Size());

    delete lfu_replacer;
}

/**
 * @brief 并发测试LFUReplacer
 * @note lab1 计分：10 points
 */
TEST(LFUReplacerTest, ConcurrencyTest) {
    const int num_threads = 5;
    const int num_runs = 50;

    for (int run = 0; run < num_runs; run++) {
        int value_size = 1000;
        std::shared_ptr<LFUReplacer> lfu_replacer{new LFUReplacer(value_size)};
        std::vector<std::thread> threads;
        std::vector<int> value(value_size);
        for (int i = 0; i < value_size; i++) {
            value[i] = i;
        }

        std::shuffle(value.begin(), value.end(), std::default_random_engine{});

        // 多线程分批 unpin 页面
        for (int tid = 0; tid < num_threads; tid++) {
            threads.emplace_back([tid, &lfu_replacer, &value]() {
                int share = 1000 / num_threads;
                for (int i = 0; i < share; i++) {
                    lfu_replacer->unpin(value[tid * share + i]);
                }
            });
        }

        for (auto &t : threads) {
            t.join();
        }

        std::vector<int> out_values;
        int result;
        for (int i = 0; i < value_size; i++) {
            EXPECT_TRUE(lfu_replacer->victim(&result));
            out_values.push_back(result);
        }

        std::sort(value.begin(), value.end());
        std::sort(out_values.begin(), out_values.end());

        EXPECT_EQ(value, out_values);
        EXPECT_FALSE(lfu_replacer->victim(&result));  // 应无剩余
    }
}
