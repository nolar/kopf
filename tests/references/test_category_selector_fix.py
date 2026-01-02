"""
Test case to verify category selector includes all versions, not just preferred ones.
This test prevents regression of the bug where category selectors only matched
preferred versions of resources.
"""

import pytest
from kopf._cogs.structs.references import Resource, Selector


def test_category_selector_includes_all_versions():
    """
    Test that category selectors include all resources with matching categories,
    regardless of whether they are the preferred version or not.
    
    This is a regression test for the bug where only preferred versions
    were selected when using category-based selectors.
    """
    
    # Create two resources with the same category but different versions
    # Only one should be preferred (as would happen in a real cluster)
    resource_v1beta1 = Resource(
        group='test.com', 
        version='v1beta1', 
        plural='rssimples',
        kind='Rssimple',
        singular='rssimple',
        categories=frozenset(['test']),
        preferred=False  # Not the preferred version
    )
    
    resource_v1beta2 = Resource(
        group='test.com',
        version='v1beta2', 
        plural='rsmultis',
        kind='Rsmulti', 
        singular='rsmulti',
        categories=frozenset(['test']),
        preferred=True  # This is the preferred version
    )
    
    # Create category selector
    category_selector = Selector(category='test')
    
    # Test that both resources match individually
    assert category_selector.check(resource_v1beta1), \
        "Non-preferred resource should match category selector"
    assert category_selector.check(resource_v1beta2), \
        "Preferred resource should match category selector"
    
    # Test selection from collection - should include BOTH resources
    all_resources = [resource_v1beta1, resource_v1beta2]
    selected_resources = category_selector.select(all_resources)
    
    assert len(selected_resources) == 2, \
        f"Expected 2 resources to be selected, but got {len(selected_resources)}"
    
    # Verify both resources are in the selection
    selected_plurals = {r.plural for r in selected_resources}
    assert selected_plurals == {'rssimples', 'rsmultis'}, \
        f"Expected both resource types, but got {selected_plurals}"


def test_non_category_selector_behavior_unchanged():
    """
    Test that non-category selectors still work as expected.
    This ensures we didn't break the existing behavior for other selector types.
    """
    
    # Create two resources from the same group with different preferences
    resource_v1beta1 = Resource(
        group='test.com', 
        version='v1beta1', 
        plural='rssimples',
        kind='Rssimple',
        singular='rssimple',
        categories=frozenset(['test']),
        preferred=False  # Not the preferred version
    )
    
    resource_v1beta2 = Resource(
        group='test.com',
        version='v1beta2', 
        plural='rsmultis',
        kind='Rsmulti', 
        singular='rsmulti',
        categories=frozenset(['test']),
        preferred=True  # This is the preferred version
    )
    
    # Create non-category selector (using kind selector)
    kind_selector = Selector(kind='Rsmulti')
    
    # Test selection - should only include the matching resource
    all_resources = [resource_v1beta1, resource_v1beta2]
    selected_resources = kind_selector.select(all_resources)
    
    assert len(selected_resources) == 1, \
        f"Expected 1 matching resource to be selected, but got {len(selected_resources)}"
    
    # Verify only the resource with matching kind is selected
    selected_resource = next(iter(selected_resources))
    assert selected_resource.kind == 'Rsmulti', \
        f"Expected resource with kind 'Rsmulti', but got '{selected_resource.kind}'"
    assert selected_resource.plural == 'rsmultis', \
        f"Expected resource 'rsmultis', but got '{selected_resource.plural}'"