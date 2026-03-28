# iX Development Guidelines

## Project Overview

This document provides guidelines for developing applications using the Siemens iX design system
packages. The libraries are available as `@siemens/ix` and related packages including
`@siemens/ix-aggrid`, `@siemens/ix-echarts`, and `@siemens/ix-icons`.

The iX design system is built on custom elements and includes comprehensive theming capabilities for
consistent styling across industrial and enterprise applications.

## Development Approach

Always search iX documentation first before implementing any UI components or patterns. This
includes looking up component APIs, design patterns, accessibility guidelines, and theming options
from the official documentation.

Prefer using existing iX components and patterns over implementing custom solutions. Follow the
established design principles of atomic design, composition over inheritance, and
accessibility-first development. Use the iX theme system with CSS custom properties rather than
hardcoded styles.

## Code Quality

Write minimal comments only where necessary, focusing on why rather than what. Avoid meta-comments
about changes or obvious explanations. Don't add unnecessary error handling for well-established iX
component APIs and assume components work as documented.

Use iX design tokens and CSS custom properties from the theme system when available instead of
writing custom CSS. Don't override iX component internals - use documented APIs, props, and CSS
custom properties for customization. Leverage the established color tokens, spacing system, and
layout patterns.

Use modern HTML5 and JavaScript features and avoid deprecated syntax. Maintain type safety with iX
component interfaces and event types.

## Package Management and Testing

Install iX packages using the appropriate package manager and follow the integration guides for each
framework. Fix import paths when packages exist but imports fail - check for correct package names
and component exports.

Run available lint, format, and test commands from package.json scripts.

## Implementation Strategy

When building applications, research iX layout patterns and navigation components first. Plan
component structure using iX building blocks like cards, buttons, modals, and navigation elements.
Create appropriate mock data that reflects real industrial/enterprise use cases.

Apply iX themes consistently throughout the application. Use the documented layout patterns and
component compositions. Verify accessibility compliance using iX's built-in accessibility features
and test with screen readers and keyboard navigation.

Follow the established iX component lifecycle patterns for events, state management, and data
binding. Use documented event handlers and avoid direct DOM manipulation of iX components.

## Theming and Customization

Use the iX theme system with CSS custom properties for customization. Available themes include
classic-light, classic-dark, and brand-specific variants not available to non-Siemens projects.
Apply themes at the application level and use theme-aware CSS custom properties for consistent
styling.

Don't override iX component styles directly. Instead, use the documented CSS custom properties,
theme tokens, and layout utilities. Create custom themes following the established token structure
when brand customization is required.

## Testing and Quality Assurance

Test iX component integration using the established testing patterns. Refrain from testing iX
component internals. Use ARIA properties in case you need access to shadow DOM elements, instead of
custom selectors. Verify functionality across different browsers and devices using iX's browser
support matrix.

Ensure accessibility compliance by testing with screen readers, keyboard navigation, and following
WCAG 2.1 AA guidelines that iX components are designed to meet. Use iX's built-in accessibility
features rather than implementing custom solutions.

This approach ensures high-quality development within the iX ecosystem while maintaining modern
framework practices and leveraging the full design system capabilities for industrial and enterprise
applications.
