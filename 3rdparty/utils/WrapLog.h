#ifndef _MY_LOG_H
#define _MY_LOG_H

#include <string>
#include <iostream>
#include <memory>

#include "spdlog/spdlog.h"

class WrapLog
{
public:
    typedef std::shared_ptr<WrapLog> PTR;

    enum ConsoleAttribute
    {
        CONSOLE_INTENSE,
        CONSOLE_NO_INTENSE,
        CONSOLE_BLACK,
        CONSOLE_WHITE,
        CONSOLE_RED,
        CONSOLE_GREEN,
        CONSOLE_BLUE,
        CONSOLE_YELLOW,
        CONSOLE_MAGENTA,
        CONSOLE_CYAN
    };

public:
    WrapLog()
    {
        mLogggers = spdlog::stdout_logger_mt("A");
        _handle = GetStdHandle(STD_OUTPUT_HANDLE);
        mCurrentColor = CONSOLE_INTENSE;

        setColor(CONSOLE_GREEN);
        mLevel = spdlog::level::info;
    }

    void                            setFile(std::string name, std::string fileName)
    {
        std::vector<spdlog::sink_ptr> sinks;
        sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_st>());
        sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>(fileName, "txt", 0, 0, true));
        mLogggers = std::make_shared<spdlog::logger>(name, begin(sinks), end(sinks));
    }

    void    setLevel(spdlog::level::level_enum num)
    {
        mLogggers->set_level(num);
        mLevel = num;
    }

    template <typename... Args> void debug(const char* fmt, const Args&... args)
    {
        if (!shouldLog(spdlog::level::debug))
        {
            return;
        }

        setColor(CONSOLE_CYAN);
        try
        {
            mLogggers->debug(fmt, std::forward<const Args&>(args)...);
        }
        catch (const spdlog::spdlog_ex& ex)
        {
            _error(ex.what());
        }
    }

    template <typename... Args> void info(const char* fmt, const Args&... args)
    {
        if (!shouldLog(spdlog::level::info))
        {
            return;
        }

        setColor(CONSOLE_GREEN);
        try
        {
            mLogggers->info(fmt, std::forward<const Args&>(args)...);
        }
        catch (const spdlog::spdlog_ex& ex)
        {
            _error(ex.what());
        }
    }
    template <typename... Args> void warn(const char* fmt, const Args&... args)
    {
        if (!shouldLog(spdlog::level::warn))
        {
            return;
        }

        setColor(CONSOLE_YELLOW);
        try
        {
            mLogggers->warn(fmt, std::forward<const Args&>(args)...);
        }
        catch (const spdlog::spdlog_ex& ex)
        {
            _error(ex.what());
        }
    }
    template <typename... Args> void error(const char* fmt, const Args&... args)
    {
        if (!shouldLog(spdlog::level::err))
        {
            return;
        }

        setColor(CONSOLE_RED);
        try
        {
            mLogggers->error(fmt, std::forward<const Args&>(args)...);
        }
        catch (const spdlog::spdlog_ex& ex)
        {
            _error(ex.what());
        }
    }

    spdlog::details::line_logger    debug()
    {
        return mLogggers->debug();
    }
    spdlog::details::line_logger    info()
    {
        return mLogggers->info();
    }
    spdlog::details::line_logger    warn()
    {
        return mLogggers->warn();
    }
    spdlog::details::line_logger    error()
    {
        return mLogggers->error();
    }
private:
    void                            _error(const char* what)
    {
        setColor(CONSOLE_RED);
        try
        {
            mLogggers->error("{}", what);
        }
        catch (const spdlog::spdlog_ex& ex)
        {
            std::cout << ex.what() << std::endl;
        }
    }

private:
    void                            setColor(ConsoleAttribute ca)
    {
        if (ca != mCurrentColor)
        {
            WORD word(colorToWORD(ca));
            SetConsoleTextAttribute(_handle, word);

            mCurrentColor = ca;
        }
    }
    WORD                            colorToWORD(ConsoleAttribute ca)
    {
        CONSOLE_SCREEN_BUFFER_INFO screen_infos;
        WORD word, flag_instensity;
        GetConsoleScreenBufferInfo(_handle, &screen_infos);

        word = screen_infos.wAttributes;

        flag_instensity = word & FOREGROUND_INTENSITY;

        switch (ca)
        {
            case CONSOLE_BLACK:
                word = 0;
                break;
            case CONSOLE_WHITE:
                word = FOREGROUND_RED | FOREGROUND_GREEN | FOREGROUND_BLUE;
                break;
            case CONSOLE_RED:
                word = FOREGROUND_RED;
                break;
            case CONSOLE_GREEN:
                word = FOREGROUND_GREEN;
                break;
            case CONSOLE_BLUE:
                word = FOREGROUND_BLUE;
                break;
            case CONSOLE_YELLOW:
                word = FOREGROUND_RED | FOREGROUND_GREEN;
                break;
            case CONSOLE_MAGENTA:
                word = FOREGROUND_RED | FOREGROUND_BLUE;
                break;
            case CONSOLE_CYAN:
                word = FOREGROUND_GREEN | FOREGROUND_BLUE;
                break;
        }

        word |= flag_instensity;

        return word;
    }

    bool    shouldLog(spdlog::level::level_enum num) const
    {
        return num >= mLevel;
    }

private:
    std::shared_ptr<spdlog::logger> mLogggers;
    HANDLE                          _handle;
    spdlog::level::level_enum       mLevel;
    ConsoleAttribute                mCurrentColor;
};

#endif