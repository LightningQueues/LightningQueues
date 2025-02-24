using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;

namespace LightningQueues.Logging;

public class RecordingLogger : ILogger
{
    private readonly LogLevel _level;
    private readonly TextWriter _console;
    private readonly IList<string> _debug = new List<string>();
    private readonly IList<string> _error = new List<string>();
    private readonly IList<string> _info = new List<string>();

    public RecordingLogger(TextWriter console, LogLevel logLevel = LogLevel.Debug)
    {
        _console = console;
        _level = logLevel;
    }

    public IEnumerable<string> DebugMessages => _debug;
    public IEnumerable<string> InfoMessages => _info;

    public IEnumerable<string> ErrorMessages => _error;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
        Func<TState, Exception, string> formatter)
    {
        var list = logLevel switch
        {
            LogLevel.Debug => _debug,
            LogLevel.Information => _info,
            LogLevel.Error => _error,
            _ => throw new ArgumentOutOfRangeException(nameof(logLevel))
        };
        var message = formatter(state, exception);
        list.Add(message);
        _console.WriteLine(message);
    }

    public bool IsEnabled(LogLevel logLevel) => logLevel switch
    {
        LogLevel.Debug when _level == LogLevel.Debug => true,
        LogLevel.Information when _level is LogLevel.Debug or LogLevel.Information => true,
        LogLevel.Error when _level is LogLevel.Debug 
            or LogLevel.Information 
            or LogLevel.Error => true,
        _ => false
    };

    public IDisposable BeginScope<TState>(TState state) where TState : notnull
    {
        return null;
    }
}