package github

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	DefaultRepo = "vogler75/monster-mq"
	UserAgent   = "MonsterMQ-Installer/1.0"
)

// Release represents a GitHub release entry.
type Release struct {
	TagName     string    `json:"tag_name"`
	Name        string    `json:"name"`
	Body        string    `json:"body"`
	Draft       bool      `json:"draft"`
	Prerelease  bool      `json:"prerelease"`
	PublishedAt time.Time `json:"published_at"`
	BrokerZip   *Asset    `json:"broker_zip,omitempty"`
	Assets      []Asset   `json:"assets"`
}

// Asset represents a release asset (e.g. zip bundle).
type Asset struct {
	Name               string `json:"name"`
	Size               int64  `json:"size"`
	DownloadURL        string `json:"browser_download_url"`
	ContentType        string `json:"content_type"`
}

// Client interacts with GitHub Releases.
type Client struct {
	Repo       string
	httpClient *http.Client
}

// NewClient returns a new GitHub client.
func NewClient(repo string) *Client {
	if repo == "" {
		repo = DefaultRepo
	}
	return &Client{
		Repo: repo,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// FetchReleases returns available releases sorted by newest first.
func (c *Client) FetchReleases() ([]Release, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases", c.Repo)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := c.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		// Fallback: try resolving latest release directly
		latest, fallbackErr := c.fetchLatestFallback()
		if fallbackErr == nil {
			return []Release{*latest}, nil
		}
		if err != nil {
			return nil, fmt.Errorf("network error fetching releases: %w", err)
		}
		return nil, fmt.Errorf("GitHub API responded with status: %s", resp.Status)
	}
	defer resp.Body.Close()

	var releases []Release
	if err := json.NewDecoder(resp.Body).Decode(&releases); err != nil {
		return nil, fmt.Errorf("failed to parse releases JSON: %w", err)
	}

	// Filter and locate broker-zip
	var validReleases []Release
	for i := range releases {
		rel := &releases[i]
		if rel.Draft {
			continue
		}
		for _, a := range rel.Assets {
			if strings.HasPrefix(a.Name, "monstermq-broker-") && strings.HasSuffix(a.Name, ".zip") {
				assetCopy := a
				rel.BrokerZip = &assetCopy
				break
			}
		}
		if rel.BrokerZip != nil {
			validReleases = append(validReleases, *rel)
		}
	}

	if len(validReleases) == 0 {
		latest, fallbackErr := c.fetchLatestFallback()
		if fallbackErr == nil {
			return []Release{*latest}, nil
		}
	}

	return validReleases, nil
}

// fetchLatestFallback follows GitHub redirect to determine latest tag name without API auth.
func (c *Client) fetchLatestFallback() (*Release, error) {
	redirectClient := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // don't follow, inspect Location
		},
		Timeout: 10 * time.Second,
	}

	url := fmt.Sprintf("https://github.com/%s/releases/latest", c.Repo)
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)

	resp, err := redirectClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	loc := resp.Header.Get("Location")
	if loc == "" {
		return nil, fmt.Errorf("no location header in latest release redirect")
	}

	parts := strings.Split(strings.TrimRight(loc, "/"), "/")
	tag := parts[len(parts)-1]
	if tag == "" || tag == "latest" {
		return nil, fmt.Errorf("could not extract tag from location %s", loc)
	}

	versionNum := strings.TrimPrefix(tag, "v")
	zipName := fmt.Sprintf("monstermq-broker-%s.zip", versionNum)
	downloadURL := fmt.Sprintf("https://github.com/%s/releases/download/%s/%s", c.Repo, tag, zipName)

	return &Release{
		TagName:     tag,
		Name:        fmt.Sprintf("MonsterMQ %s", tag),
		Body:        "Fetched from GitHub releases.",
		PublishedAt: time.Now(),
		BrokerZip: &Asset{
			Name:        zipName,
			DownloadURL: downloadURL,
		},
	}, nil
}
