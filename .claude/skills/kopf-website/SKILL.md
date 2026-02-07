---
name: kopf-website
description: Use when creating or editing pages under website/. Provides the style guide with colors, typography, layout, and component patterns for kopf.dev.
---

# kopf.dev Website Style Guide

Location: `website/` directory in the kopf repo.
Tech: Plain HTML + Tailwind CSS (CDN) + highlight.js. No build step.

## CDN Dependencies (copy into every page `<head>`)

```html
<script src="https://cdn.tailwindcss.com"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/python.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/dockerfile.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/bash.min.js"></script>
```

## Tailwind Config (copy into every page `<head>`)

```html
<script>
  tailwind.config = {
    theme: {
      extend: {
        fontFamily: {
          sans: ['Inter', 'system-ui', 'sans-serif'],
          mono: ['JetBrains Mono', 'monospace'],
        },
        colors: {
          kopf: {
            50: '#eef7ff',
            100: '#d9edff',
            200: '#bce0ff',
            300: '#8eccff',
            400: '#59b0ff',
            500: '#3490fc',
            600: '#1e72f1',  // Primary brand color
            700: '#165bde',
            800: '#184ab4',
            900: '#1a418e',
            950: '#142956',
          },
        },
      },
    },
  }
</script>
```

## Custom CSS (copy into every page `<head>`)

```html
<style>
  html { scroll-behavior: smooth; }
  .hero-gradient {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #142956 100%);
  }
  .code-window {
    background: #0d1117;
    border: 1px solid #30363d;
  }
  .code-window-bar {
    background: #161b22;
    border-bottom: 1px solid #30363d;
  }
  .feature-card {
    transition: transform 0.2s ease, box-shadow 0.2s ease;
  }
  .feature-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 12px 24px -8px rgba(0, 0, 0, 0.15);
  }
  pre code.hljs {
    background: transparent !important;
    padding: 0 !important;
  }
  .tab-btn.active {
    color: #3490fc;
    border-color: #3490fc;
  }
  .tab-content { display: none; }
  .tab-content.active { display: block; }
</style>
```

## Colors

- **Primary (brand blue):** `kopf-600` (#1e72f1) for buttons, accents, links
- **Primary hover:** `kopf-500` (#3490fc) or `kopf-700` (#165bde)
- **Hero/CTA dark backgrounds:** `.hero-gradient` class (slate-900 to kopf-950)
- **Light text on dark:** `text-white`, `text-slate-300`, `text-kopf-300`, `text-kopf-400`
- **Body text:** `text-slate-900` (headings), `text-slate-600` (body), `text-slate-500` (captions)
- **Alternating section backgrounds:** `bg-white` and `bg-slate-50`
- **Feature card icon backgrounds:** use different Tailwind color-100 tints (kopf-100, emerald-100, amber-100, violet-100, rose-100, cyan-100, teal-100, orange-100, indigo-100) with matching color-600 icons

## Typography

- **Font:** Inter (sans), JetBrains Mono (code)
- **Body base:** `class="bg-white text-slate-900 font-sans antialiased"`
- **Page titles (h1):** `text-4xl md:text-5xl lg:text-6xl font-extrabold leading-tight tracking-tight`
- **Section headings (h2):** `text-3xl md:text-4xl font-bold tracking-tight`
- **Card/subsection headings (h3):** `font-semibold text-lg`
- **Section subtext:** `mt-4 text-lg text-slate-600`
- **Body/card text:** `text-slate-600 text-sm leading-relaxed`
- **Captions:** `text-sm text-slate-500`
- **Inline code:** `<code class="text-xs bg-slate-100 px-1.5 py-0.5 rounded">...</code>`

## Layout

- **Max width:** `max-w-6xl mx-auto px-6`
- **Section padding:** `py-20 md:py-28`
- **Section header pattern:** centered `max-w-2xl mx-auto mb-16` with h2 + p
- **Feature grids:** `grid md:grid-cols-2 lg:grid-cols-3 gap-6`
- **Two-column layouts:** `grid lg:grid-cols-2 gap-12 items-center`

## Components

### Navigation (fixed top bar)
```html
<nav class="fixed top-0 w-full z-50 bg-white/80 backdrop-blur-lg border-b border-slate-200">
  <div class="max-w-6xl mx-auto px-6 h-16 flex items-center justify-between">
    <!-- Logo left, nav links center (hidden md:flex), actions right -->
  </div>
</nav>
```
- Nav links: `text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors`
- CTA button in nav: `px-4 py-2 rounded-lg bg-kopf-600 text-white text-sm font-medium hover:bg-kopf-700`

### Logo SVG
```html
<svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
  <rect width="32" height="32" rx="8" fill="#1e72f1"/>
  <text x="16" y="22" text-anchor="middle" fill="white" font-size="18" font-weight="bold" font-family="Inter, sans-serif">K</text>
</svg>
```

### Code Windows (GitHub-dark style)
```html
<div class="code-window rounded-xl overflow-hidden shadow-lg">
  <div class="code-window-bar px-4 py-3 flex items-center gap-2">
    <span class="w-3 h-3 rounded-full bg-[#ff5f57]"></span>
    <span class="w-3 h-3 rounded-full bg-[#febc2e]"></span>
    <span class="w-3 h-3 rounded-full bg-[#28c840]"></span>
    <span class="ml-3 text-xs text-slate-400 font-mono">filename.py</span>
  </div>
  <div class="p-5 text-sm leading-relaxed overflow-x-auto">
    <pre><code class="language-python">...</code></pre>
  </div>
</div>
```
- Smaller code blocks (no title bar): omit `.code-window-bar`, use `rounded-lg` and `p-4`

### Feature Cards
```html
<div class="feature-card p-6 rounded-xl border border-slate-200 bg-white">
  <div class="w-10 h-10 rounded-lg bg-{color}-100 flex items-center justify-center mb-4">
    <svg class="w-5 h-5 text-{color}-600" ...>...</svg>
  </div>
  <h3 class="font-semibold text-lg">Title</h3>
  <p class="mt-2 text-slate-600 text-sm leading-relaxed">Description</p>
</div>
```

### Buttons
- **Primary:** `inline-flex items-center gap-2 px-6 py-3 rounded-lg bg-kopf-600 text-white font-semibold hover:bg-kopf-500 transition-colors`
- **Secondary (on dark bg):** `inline-flex items-center gap-2 px-6 py-3 rounded-lg bg-white/10 text-white font-semibold hover:bg-white/20 transition-colors`
- **Arrow icon for CTAs:**
  ```html
  <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 8h10M9 4l4 4-4 4"/></svg>
  ```

### Checkmark Lists
```html
<li class="flex items-start gap-3">
  <svg class="w-5 h-5 text-emerald-500 mt-0.5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg>
  <span class="text-slate-700">Item text</span>
</li>
```

### Numbered Steps
```html
<div class="flex gap-6">
  <div class="flex-shrink-0 w-10 h-10 rounded-full bg-kopf-600 flex items-center justify-center text-white font-bold">1</div>
  <div class="flex-1 min-w-0">
    <h3 class="font-semibold text-lg">Step title</h3>
    <!-- content -->
  </div>
</div>
```

### Tabs
- Tab bar: `flex gap-1 mb-6 border-b border-slate-200 overflow-x-auto`
- Tab buttons: `tab-btn px-4 py-2.5 text-sm font-medium text-slate-500 border-b-2 border-transparent whitespace-nowrap transition-colors`
- Active state via `.active` class (CSS handles color/border)
- Content panels: `tab-content` class, toggle `.active` via JS

### Footer
- Background: `bg-slate-900 text-slate-400 py-12`
- Grid: `grid md:grid-cols-4 gap-8`
- Columns: Kopf (logo + tagline), Resources, Community, Legal
- Links: `hover:text-white transition-colors`
- **Required:** The Legal column must include `<a href="/impressum/" class="hover:text-white transition-colors">Impressum</a>`

## GitHub Icon SVG
```html
<svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor">
  <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/>
</svg>
```

## Page Template

Every new page should follow this structure:
1. Same `<head>` block (CDN deps, Tailwind config, custom CSS)
2. Same `<nav>` (update active link if applicable)
3. Content sections alternating `bg-white` / `bg-slate-50`
4. Same `<footer>`
5. `<script>hljs.highlightAll();</script>` at the bottom

## External Links

- Docs: https://kopf.readthedocs.io/
- GitHub: https://github.com/nolar/kopf
- PyPI: https://pypi.org/project/kopf/
- Examples: https://github.com/nolar/kopf/tree/main/examples
- Contributing: https://github.com/nolar/kopf/blob/main/CONTRIBUTING.md
- Issues: https://github.com/nolar/kopf/issues
- Discussions: https://github.com/nolar/kopf/discussions

All external links use `target="_blank" rel="noopener"`.
