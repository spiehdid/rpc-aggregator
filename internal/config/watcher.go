package config

import (
	"log/slog"

	"github.com/fsnotify/fsnotify"
)

type Watcher struct {
	watcher  *fsnotify.Watcher
	path     string
	callback func(*Config)
	done     chan struct{}
}

func NewWatcher(path string, callback func(*Config)) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := fsw.Add(path); err != nil {
		fsw.Close()
		return nil, err
	}
	w := &Watcher{watcher: fsw, path: path, callback: callback, done: make(chan struct{})}
	go w.watch()
	return w, nil
}

func (w *Watcher) watch() {
	defer close(w.done)
	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				cfg, err := Load(w.path)
				if err != nil {
					slog.Error("config reload failed", "error", err)
					continue
				}
				slog.Info("config reloaded successfully")
				w.callback(cfg)
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			slog.Error("config watcher error", "error", err)
		}
	}
}

func (w *Watcher) Close() error {
	err := w.watcher.Close()
	<-w.done
	return err
}
