package stoplist

import "testing"

func TestStoplist(t *testing.T) {
	sl := New("badword")

	if !sl.Contains("badword") {
		t.Fatal("expected exact match in stoplist")
	}
	if !sl.Contains("something badword here") {
		t.Fatal("expected substring match")
	}
	if sl.Contains("iphone") {
		t.Fatal("unexpected match")
	}

	if !sl.Add("spam") {
		t.Fatal("expected add success")
	}
	if sl.Add("spam") {
		t.Fatal("duplicate add should return false")
	}

	if !sl.Remove("spam") {
		t.Fatal("expected remove success")
	}
	if sl.Remove("spam") {
		t.Fatal("second remove should return false")
	}
}
