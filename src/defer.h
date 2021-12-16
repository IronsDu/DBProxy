#ifndef _DBPROXY_DEFER_H
#define _DBPROXY_DEFER_H

template<typename F>
struct privDefer {
    F f;
    privDefer(F f)
        : f(std::move(f))
    {}
    ~privDefer()
    {
        f();
    }
};

template<typename F>
privDefer<F> defer_func(F f)
{
    return privDefer<F>(std::move(f));
}

#define DEFER_1(x, y) x##y
#define DEFER_2(x, y) DEFER_1(x, y)
#define DEFER_3(x) DEFER_2(x, __COUNTER__)
#define defer(code) auto DEFER_3(_defer_) = defer_func([&]() { \
                        code;                                  \
                    })

#endif
