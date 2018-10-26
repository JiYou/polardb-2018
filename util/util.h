#pragma once

#define WITH_NO_COPY_CLASS(C)     private:  \
    C (const C &&p) = delete;               \
    C (const C& p) = delete;                \
    C &operator=(const C& p) = delete;      \
    C &operator=(C &&p) = delete

#define WITH_DEFAULT_CLASS(C)     public:    \
    C ( C &&p) = default;                    \
    C (const C& p) = default;                \
    C &operator=(const C& p) = default;      \
    C &operator=(C &&p) = default

#define UNUSED(expr) do { (void)(expr); } while (0)