require 'test_helper'

class SogousControllerTest < ActionController::TestCase
  setup do
    @sogou = sogous(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:sogous)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create sogou" do
    assert_difference('Sogou.count') do
      post :create, sogou: {  }
    end

    assert_redirected_to sogou_path(assigns(:sogou))
  end

  test "should show sogou" do
    get :show, id: @sogou
    assert_response :success
  end

  test "should get edit" do
    get :edit, id: @sogou
    assert_response :success
  end

  test "should update sogou" do
    patch :update, id: @sogou, sogou: {  }
    assert_redirected_to sogou_path(assigns(:sogou))
  end

  test "should destroy sogou" do
    assert_difference('Sogou.count', -1) do
      delete :destroy, id: @sogou
    end

    assert_redirected_to sogous_path
  end
end
