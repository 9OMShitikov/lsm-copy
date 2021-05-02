package lsm

func fatalOnErr(err error) {
	if err == nil {
		return
	}
	panic("Fatal error: " + err.Error())
}

func assert(cond bool) {
	if cond {
		return
	}
	panic("Fatal assertion error")
}

// FirstErr selects the first non-nil error from the list
func FirstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (lsm *Lsm) debug(fmt string, args ...interface{}) {
	lsm.cfg.Log.Debugf(fmt, args...)
}

func (lsm *Lsm) debug2(fmt string, args ...interface{}) {
	//lsm.cfg.Log.Debug(fmt, args...)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// nolint:unused,deadcode
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
