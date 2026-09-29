import { defineConfig } from "vitepress";

// GitHub Pages 项目站挂在 /jsonstream/ 下：CI 里从 GITHUB_REPOSITORY 推 base，
// 本地 dev/preview 用 "/"，两边一套配置。（同款推导见 falcon 的站点。）
const repository = process.env.GITHUB_REPOSITORY ?? "";
const repositoryName = repository.split("/")[1] ?? "";
const isUserOrOrgPagesRepo = repositoryName.endsWith(".github.io");
const base =
  process.env.GITHUB_ACTIONS === "true" && repositoryName && !isUserOrOrgPagesRepo
    ? `/${repositoryName.toLowerCase()}/`
    : "/";

export default defineConfig({
  lang: "zh-CN",
  title: "JsonStream",
  titleTemplate: false,
  description:
    "基于 TCP 的自定义 JSON 二进制帧协议——一道面试题的衍生实现，纯标准库 Go 参考实现，协议规范语言无关。",
  base,
  cleanUrls: true,
  lastUpdated: true,
  outDir: ".vitepress/dist",
  themeConfig: {
    siteTitle: "JsonStream",
    logo: "/logo.svg",
    nav: [
      { text: "上手指南", link: "/getting-started" },
      { text: "API 参考", link: "/api" },
      { text: "协议规范", link: "/protocol" },
      { text: "术语速查", link: "/glossary" },
      { text: "题目要求", link: "/interview-requirements" },
    ],
    sidebar: {
      "/": [
        {
          text: "上手",
          items: [
            { text: "上手指南", link: "/getting-started" },
            { text: "API 参考", link: "/api" },
            { text: "协议术语速查", link: "/glossary" },
            { text: "基准实测", link: "/benchmarks" },
            { text: "故障排查 / FAQ", link: "/faq" },
          ],
        },
        {
          text: "项目背景",
          items: [
            { text: "面试题要求", link: "/interview-requirements" },
            { text: "更新日志", link: "/changelog" },
          ],
        },
        {
          text: "协议与设计",
          items: [
            { text: "协议规范（单一事实源）", link: "/protocol" },
            { text: "与 WebSocket 的能力对照", link: "/websocket-comparison" },
            { text: "TCP 流特性与协议横评", link: "/tcp-and-landscape" },
            { text: "架构与取舍", link: "/DESIGN" },
            { text: "设计笔记", link: "/design-notes" },
            { text: "知识点梳理", link: "/NOTES" },
          ],
        },
      ],
    },
    socialLinks: [
      { icon: "github", link: "https://github.com/cuihairu/jsonstream" },
    ],
    footer: {
      message: "基于 MIT 许可发布",
      copyright: "Copyright © 2024-present cuihairu",
    },
    outline: {
      level: [2, 3],
      label: "页面导航",
    },
    search: {
      provider: "local",
    },
    docFooter: {
      prev: "上一页",
      next: "下一页",
    },
    lastUpdated: {
      text: "最后更新于",
    },
  },
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: `${base}favicon.svg` }],
    ["meta", { name: "theme-color", content: "#e16531" }],
    ["meta", { property: "og:type", content: "website" }],
    ["meta", { property: "og:locale", content: "zh-CN" }],
    ["meta", { property: "og:title", content: "JsonStream" }],
    [
      "meta",
      {
        property: "og:description",
        content:
          "基于 TCP 的自定义 JSON 二进制帧协议——一道面试题的衍生实现，纯标准库 Go 参考实现，协议规范语言无关。",
      },
    ],
  ],
});
