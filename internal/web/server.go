package web

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"time"

	"github.com/zelmario/pt-mongodb-query-digest/internal/aggregator"
	"github.com/zelmario/pt-mongodb-query-digest/internal/report"
)

// Serve renders the report once and serves it on addr until ctx is cancelled.
// If openBrowser is true, tries to launch the default browser pointing at the
// bound URL once the listener is ready.
func Serve(ctx context.Context, addr string, openBrowser bool, rctx report.Context, sums []*aggregator.Summary, limit int) error {
	html, err := Render(rctx, sums, limit)
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(html)
	})

	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	url := fmt.Sprintf("http://%s/", ln.Addr().String())
	fmt.Fprintf(errLogger, "pt-mongodb-query-digest: serving %s\n", url)
	fmt.Fprintf(errLogger, "pt-mongodb-query-digest: press Ctrl+C to stop.\n")

	if openBrowser {
		go func() {
			time.Sleep(200 * time.Millisecond)
			if err := openURL(url); err != nil {
				fmt.Fprintf(errLogger, "(could not auto-open browser: %v)\n", err)
			}
		}()
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// openURL launches the default browser on the given URL. Silent-failing is
// fine; the user can always copy-paste from the printed message.
func openURL(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}
