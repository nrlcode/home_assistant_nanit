package utils_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestGracefulRunner(t *testing.T) {
	var mu sync.Mutex
	out := ""

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			<-childCtx.Done()
			time.Sleep(500 * time.Millisecond)
			mu.Lock()
			out = out + " sub_finished"
			mu.Unlock()
		})

		<-ctx.Done()
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		out = out + " main_finished"
		mu.Unlock()
	})

	time.Sleep(100 * time.Millisecond)
	runner.Cancel()
	// Wait for runner to complete before appending
	runner.Wait()
	mu.Lock()
	out = out + " after_cancel"
	mu.Unlock()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, " main_finished sub_finished after_cancel", out)
}

func TestGracefulRunnerFinished(t *testing.T) {
	var mu sync.Mutex
	out := ""
	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			time.Sleep(300 * time.Millisecond)
			mu.Lock()
			out = out + " sub_finished"
			mu.Unlock()
		})

		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		out = out + " main_finished"
		mu.Unlock()
	})

	_, err := runner.Wait()
	assert.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, " main_finished sub_finished", out)
}

func TestGracefulRunnerFail(t *testing.T) {
	var mu sync.Mutex
	out := ""
	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			<-childCtx.Done()
			time.Sleep(200 * time.Millisecond)
			mu.Lock()
			out = out + " sub_finished"
			mu.Unlock()
		})

		for {
			select {
			case <-time.After(200 * time.Millisecond):
				ctx.Fail(errors.New("simulated failure"))
			case <-ctx.Done():
				time.Sleep(100 * time.Millisecond)
				mu.Lock()
				out = out + " main_finished"
				mu.Unlock()
				return
			}
		}
	})

	_, err := runner.Wait()
	assert.EqualError(t, err, "simulated failure")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, " main_finished sub_finished", out)
}
