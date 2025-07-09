# Celeborn Web UI

Celeborn Web is a dashboard to display and manage the Master and Worker of Celeborn. This document introduces how to install and build the UI of Celeborn Web.

> **⚠️ Important**
>
> Before running commands, you must ensure that you are in the front-end directory `celeborn/web`. If not, run `cd web` first.

---

## Getting started

### Preparation | Framework & Dependencies

- [Vue](https://vuejs.org)

> **TIP**
>
> You should use the Pnpm package manager.
>

## Installation

### Development environment

- Run the below command in the console to install the required dependencies.

```sh
pnpm install
```

### Compile and Hot-Reload for Development

```sh
pnpm dev
```

### Type-Check, Compile and Minify for Production

```sh
pnpm build
```

### Lint with [ESLint](https://eslint.org/)

```sh
# Whole project
pnpm lint
```
