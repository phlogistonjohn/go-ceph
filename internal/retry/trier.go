package retry


// WithTries repeatedly calls a SizeFunc with increasing sizes until either it
// returns nil, or a maximum number of tries has been reached. If the returned hint is
// DoubleSize or indicating a size not greater than the current size, the size
// is doubled.
func WithTries(size int, maxTries int, f SizeFunc) {
	attempt := 0
	for {
		attempt += 1
		if attempt > maxTries {
			break
		}
		hint := f(size)
		if hint == nil {
			break
		}
		if hint.size() > size {
			size = hint.size()
		} else {
			size *= 2
		}
	}
}
