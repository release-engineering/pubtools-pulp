class PhaseInterrupted(RuntimeError):
    """The exception raised when a phase needs to stop because an earlier phase
    has encountered a fatal error."""
