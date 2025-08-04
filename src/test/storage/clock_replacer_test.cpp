#include "replacer/clock_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

/**
 * @brief 简单测试ClockReplacer的基本功能
 */
TEST(ClockReplacerTest, SimpleTest) {
    ClockReplacer clock_replacer(7);

    // Unpin几个页面
    clock_replacer.unpin(1);
    clock_replacer.unpin(2);
    clock_replacer.unpin(3);
    clock_replacer.unpin(4);
    clock_replacer.unpin(5);
    clock_replacer.unpin(6);
    clock_replacer.unpin(1);  // 再次unpin无影响

    EXPECT_EQ(6, clock_replacer.Size());
    int value;

    // victim顺序：1, 2, 3（都是第一次访问）
    EXPECT_EQ(true, clock_replacer.victim(&value));
    EXPECT_EQ(1, value);
    EXPECT_EQ(true, clock_replacer.victim(&value));
    EXPECT_EQ(2, value);
    EXPECT_EQ(true, clock_replacer.victim(&value));
    EXPECT_EQ(3, value);

    // pin 4和无效的3（已victim过）
    clock_replacer.pin(3);
    clock_replacer.pin(4);
    EXPECT_EQ(2, clock_replacer.Size());

    // unpin 4 -> 设置reference为1
    clock_replacer.unpin(4);

    // victim顺序应为5、6、4
    EXPECT_EQ(true, clock_replacer.victim(&value));
    EXPECT_EQ(5, value);
    EXPECT_EQ(true, clock_replacer.victim(&value));
    EXPECT_EQ(6, value);
    EXPECT_EQ(true, clock_replacer.victim(&value));
    EXPECT_EQ(4, value);
}

/**
 * @brief 大数据量测试ClockReplacer的稳定性
 */
TEST(ClockReplacerTest, MixTest) {
    int result;
    int value_size = 10000;
    auto clock_replacer = new ClockReplacer(value_size);
    std::vector<int> value(value_size);

    for (int i = 0; i < value_size; ++i) {
        value[i] = i;
    }
    std::shuffle(value.begin(), value.end(), std::default_random_engine{});

    for (int i = 0; i < value_size; ++i) {
        clock_replacer->unpin(value[i]);
    }

    EXPECT_EQ(value_size, clock_replacer->Size());

    // pin再unpin某些值
    clock_replacer->pin(777);
    clock_replacer->unpin(777);

    // victim掉value[0]，再unpin
    EXPECT_EQ(true, clock_replacer->victim(&result));
    int removed = result;
    clock_replacer->unpin(removed);

    // 继续pin/unpin部分值
    for (int i = 0; i < value_size / 2; i++) {
        if (value[i] != removed && value[i] != 777) {
            clock_replacer->pin(value[i]);
            clock_replacer->unpin(value[i]);
        }
    }

    // 构造victim期望序列（大致）
    std::set<int> expected;
    for (int i = 0; i < value_size; ++i) {
        expected.insert(value[i]);
    }

    int count = 0;
    std::set<int> actual;
    while (clock_replacer->victim(&result)) {
        actual.insert(result);
        count++;
    }

    EXPECT_EQ(expected, actual);
    EXPECT_EQ(0, clock_replacer->Size());

    delete clock_replacer;
}

/**
 * @brief 并发测试ClockReplacer
 */
TEST(ClockReplacerTest, ConcurrencyTest) {
    const int num_threads = 4;
    const int num_runs = 20;

    for (int run = 0; run < num_runs; run++) {
        int value_size = 500;
        std::shared_ptr<ClockReplacer> clock_replacer = std::make_shared<ClockReplacer>(value_size);
        std::vector<std::thread> threads;
        std::vector<int> values(value_size);

        for (int i = 0; i < value_size; ++i) {
            values[i] = i;
        }
        std::shuffle(values.begin(), values.end(), std::default_random_engine{});

        for (int tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([tid, &clock_replacer, &values]() {
                int share = values.size() / 4;
                for (int i = 0; i < share; ++i) {
                    clock_replacer->unpin(values[tid * share + i]);
                }
            });
        }

        for (auto &t : threads) {
            t.join();
        }

        std::set<int> expected(values.begin(), values.end());
        std::set<int> actual;
        int res;
        while (clock_replacer->victim(&res)) {
            actual.insert(res);
        }

        EXPECT_EQ(expected, actual);
        EXPECT_EQ(0, clock_replacer->Size());
    }
}